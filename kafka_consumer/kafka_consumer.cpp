
#include "vhash.h"
#include "utils.h"
#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"

#include "sink/csv.h"
#ifdef WITH_ORC
#   include "sink/orc.h"
#endif
#ifdef WITH_CH
#   include "sink/clickhouse.h"
#endif
#define ALL_PARTITIONS -1
#define REBALANCE_PARTITIONS -2
#define REBALANCE_AUTOCOMMIT true

#define POLL_TIMEOUT 30000
#define NOTINT INT_MAX


using namespace std;
using namespace cppkafka;


// kafka consumer

struct PartitionTask
{
    PartitionTask() : partition(INT_MAX), offset(-1), limit(0), isNull(true), commit(false) {}

    int partition;
    int64 offset;
    int64 limit;
    bool isNull;
    bool commit;
};

class KafkaSource
{
    Consumer *consumer;

    const std::string brokers, topic, group;
    std::vector<PartitionTask> partitions;
    int64 limit;

    Sink *sink;

    bool timedout;
    int duration;
    int maxDuration;
    bool subscribe;

public:
    KafkaSource(std::string brokers, std::string topic, std::string partitions, std::string group, Sink *sink, long long timeout)
        : consumer(NULL), brokers(brokers), topic(topic), group(group), limit(0), sink(sink), maxDuration(timeout)
    {
        parsePartitions(partitions);
    }

    void setup()
    {
        this->subscribe = partitions.size() == 1 && partitions[0].partition == REBALANCE_PARTITIONS;
        const bool autocommit = subscribe && REBALANCE_AUTOCOMMIT;

        this->timedout = false;
        this->duration = 0;

        Configuration config = {
            { "metadata.broker.list", brokers },
            { "group.id", group },
            { "enable.auto.commit", autocommit },
            { "enable.auto.offset.store", autocommit },
            { "auto.offset.reset", subscribe ? "earliest" : "error" },
        };

        consumer = new Consumer(config);

        int topicPartitionCount = getPartitionCount(consumer->get_handle(), topic.c_str());
        if (partitions.size() == 1 && (partitions[0].partition == ALL_PARTITIONS ||
                                       partitions[0].partition == REBALANCE_PARTITIONS))
        {
            limit = partitions[0].limit;
            std::vector<PartitionTask> allPartitions;
            for (int p = 0; p < topicPartitionCount; ++p)
            {
                PartitionTask t;
                t.isNull = false;
                t.partition = p;
                t.offset = partitions[0].offset;
                t.limit = 0;
                t.commit = partitions[0].commit && !autocommit;
                allPartitions.push_back(t);
            }
            partitions = allPartitions;
        }

        if (subscribe)
        {
            consumer->set_assignment_callback([&](const TopicPartitionList& partitions) {

                std::vector<PartitionTask> assignedPartitions;
                int partitionCount = 1;

                for (TopicPartition p : partitions)
                {
                    PartitionTask t;
                    t.isNull = false;
                    t.partition = p.get_partition();
                    t.offset = -1000;
                    t.limit = 0;
                    t.commit = !REBALANCE_AUTOCOMMIT;
                    assignedPartitions.push_back(t);

                    partitionCount = max(partitionCount, t.partition + 1);
                    fprintf(stderr, "partition %d: rebalance assign %s\n", t.partition, t.commit ? "manual commit" : "autocommit");
                }

                this->partitions.clear();
                this->partitions.resize(partitionCount);
                for (PartitionTask p : assignedPartitions)
                {
                    this->partitions[p.partition] = p;
                }
            });

            consumer->set_revocation_callback([&](const TopicPartitionList& partitions) {
                this->commitOffsets();
            });

            consumer->subscribe({ topic });
        }
        else
        {
            TopicPartitionList offsets;
            for (PartitionTask &p : partitions)
            {
                p.isNull = p.isNull || p.partition < 0 || p.partition >= topicPartitionCount;
                if (!p.isNull)
                {
                    offsets.push_back(TopicPartition(topic, p.partition, p.offset));
                    fprintf(stderr, "partition %d: assign %ld limit %ld\n", p.partition, p.offset, p.limit);
                }
            }

            consumer->assign(offsets);
        }
    }

    void destroy()
    {
        if (subscribe)
        {
            consumer->unsubscribe();
        }
    }

    virtual void process()
    {
        int64 currentLimit = getCurrentLimit();
        //fprintf(stderr, "Consuming messages from topic %s limit %lld", topic.c_str(), currentLimit);
        int batch_size = max(0, min((int)currentLimit, 100000));

        while ((getCurrentLimit() > 0 && !timedout && duration < maxDuration))
        {
            auto start = std::chrono::steady_clock::now();
            std::vector<Message> msgs = consumer->poll_batch(batch_size, std::chrono::milliseconds(POLL_TIMEOUT));
            auto end = std::chrono::steady_clock::now();

            auto d = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            timedout = (d >= POLL_TIMEOUT);
            duration += d;

            fprintf(stderr, "%lu messages polled: %ld ms poll, %d ms total\n", msgs.size(), d, duration);
            start = std::chrono::steady_clock::now();
            for (Message &msg: msgs)
            {
                if (!msg)
                {
                    continue;
                }

                if (msg.get_error() && !msg.is_eof())
                {
                    fprintf(stderr, "error recieived: %s\n", msg.get_error().to_string().c_str());
                    continue;
                }

                int part = msg.get_partition();

                partitions[part].limit -= 1;
                partitions[part].offset = msg.get_offset() + 1;

                sink->put(msg);
                limit -= 1;
            }
            end = std::chrono::steady_clock::now();
            duration += std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        }

        fprintf(stderr, timedout || duration > maxDuration ? "timeout\n" : "limit exceeded\n");
        sink->flush();
        commitOffsets();
    }

private:

    int getPartitionCount(rd_kafka_t* rdkafka, const char *topic_name)
    {
        int partitionCount = 0;

        rd_kafka_topic_t *rdtopic = rd_kafka_topic_new(rdkafka, topic_name, 0);
        const rd_kafka_metadata_t *rdmetadata;

        rd_kafka_resp_err_t err = rd_kafka_metadata(rdkafka, 0, rdtopic, &rdmetadata, 30000);
        if (err == RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            for (int i = 0; i < rdmetadata->topic_cnt; ++i)
            {
                partitionCount = rdmetadata->topics[i].partition_cnt;
            }
            rd_kafka_metadata_destroy(rdmetadata);
        }

        rd_kafka_topic_destroy(rdtopic);

        return partitionCount;
    }

    void parsePartitions(const std::string &partFmt)
    {
        std::map<std::string, int> partConsts;
        partConsts["*"] = ALL_PARTITIONS;
        partConsts["%"] = REBALANCE_PARTITIONS;

        int partitionCount = 1;

        std::vector<std::string> fmtParts = splitString(partFmt, ',');

        std::vector<PartitionTask> partitionList;
        for (std::string part : fmtParts)
        {
            std::vector<std::string> tuple = splitString(part, ':');

            if (tuple.size() != 3)
            {
                printf("partition format missmatch: [partition:offset:limit](,[partition:offset:limit])*");
            }

            PartitionTask t;
            t.isNull = false;
            t.partition = partConsts.count(tuple[0]) ? partConsts[tuple[0]] : toInt(tuple[0], -1);
            t.offset = toInt(tuple[1]);
            t.limit = toInt(tuple[2]);
            t.commit = t.offset < 0;

            if (t.partition < 0 && tuple[0] != std::string("*") && tuple[0] != std::string("%"))
            {
                vt_report_error(0, "partition number must be integer, '*' or '%%'");
            }
            else if (t.partition < 0 && fmtParts.size() > 1)
            {
                vt_report_error(0, "only one partition clause is expected for '*' or '%%'");
            }
            else if (t.offset == NOTINT || (t.offset < 0 && t.offset != -1 && t.offset != -2 && t.offset != -1000))
            {
                vt_report_error(0, "partition offset must be positive integer or -1 for latest or -2 for earlest or -1000 for last read");
            }
            else if (t.partition == REBALANCE_PARTITIONS && t.offset != -1000)
            {
                vt_report_error(0, "subscribe is only available with offset -1000 (last read)");
            }
            else if (t.limit == NOTINT || t.limit < 0)
            {
                vt_report_error(0, "partition limit must be positive integer");
            }
            else
            {
                partitionList.push_back(t);
                partitionCount = max(partitionCount, t.partition + 1);
            }
        }

        if (partitionCount == 1 && (partitionList[0].partition == ALL_PARTITIONS ||
                                    partitionList[0].partition == REBALANCE_PARTITIONS))
        {
            partitions = partitionList;
        }
        else
        {
            partitions.resize(partitionCount);
            for (PartitionTask p : partitionList)
            {
                partitions[p.partition] = p;
            }
        }
    }

    int64 getCurrentLimit() const
    {
        int64 partitionLimit = 0;
        for (PartitionTask p : partitions)
        {
            if (!p.isNull && p.limit > 0)
            {
                partitionLimit += p.limit;
            }
        }
        return max((int64)0, max(limit, partitionLimit));
    }

    void commitOffsets()
    {
        TopicPartitionList offsets;
        for (PartitionTask &p : partitions)
        {
            if (!p.isNull && p.commit && p.offset > 0)
            {
                offsets.push_back(TopicPartition(topic, p.partition, p.offset));
                p.offset = -1;
            }
        }
        if (!offsets.empty())
        {
            consumer->store_offsets(offsets);
            consumer->commit(offsets);
        }
    }
};


int main(int argc, char **argv)
{
    string brokers;
    string topic;
    string group;
    string task;
    long long timeout = 3 * POLL_TIMEOUT;

    string filename;
    string format;
    string columns;

    string host;
    int port = -1;
    string user;
    string password;
    string database;
    string table;

    for (int i = 1; i < argc - 1; ++i)
    {
        if (strcmp(argv[i], "--help") == 0)
        {
            printf("%s \n"
                   " --brokers clickstream-kafka01:9092 \n"
                   " --topic user-keyed-clickstream \n"
                   " --group test-group \n"
                   " --task \"%%:-2:1000000\" \n"
                   " --filename file.orc \n"
                   " --format orc \n"
                   " --columns src_id:bigint,src:bigint,u:string,uid:bigint,eid:bigint,dt:bigint,dtm:float \n"
                   , argv[0]);

            //clickhouse: "ref:String,u:String,ua:String,url:String,x:String,cid:UInt32,dt:UInt32,eid:UInt32,iid:UInt32,lid:UInt32,mcid:UInt32,src:UInt32,src_id:UInt32,uid:UInt32,dtm:Float64"
            //csv: "timestamp;columns:hash(uuid),hash(u),uid,eid,dt,dtm,src_id,src,hash(ua)"
            //orc: "business_platform:bigint,src_id:bigint,err:bigint,ip:string,uid:bigint,ref:string,v:bigint,src:bigint,u:string,ab:string,bot:boolean,du:string,eid:bigint,geo:string,ua:string,url:string,app:bigint,dt:bigint,bt:bigint,email:string,bool_param:boolean,lid:bigint,cid:bigint,q:string,offset:bigint,limit:bigint,total:bigint,oid:bigint,mcid:bigint,aid:bigint,buid:bigint,iid:bigint,mid:string,fid:bigint,tx:string,st:bigint,uem:string,type:string,vin:string,params:string,item_id:bigint,categoryid:bigint,campaign_id:string,hide_phone:boolean,group_id:string,launch_id:string,phone:string,esid:string,sid:bigint,name:string,title:string,price:double,photos:bigint,photos_ids:string,user_id:bigint,engine:string,puid:bigint,msg:string,manager:string,addr:string,phone1:string,inn:bigint,addr2:string,addr1:string,addrcp:boolean,token:string,success:boolean,source:bigint,engine_version:string,quick:boolean,cmm:string,from:string,status_id:bigint,typeid:bigint,position:bigint,reason:string,items:bigint,tid:bigint,vsrc:string,imgid:bigint,x:string,cm:string,errors_detailed:string,shopId:bigint,prob:string,screen:string,vid:bigint,rules:string,lf_cnt:bigint,orderid:bigint,_ctid:bigint,cids:string,img:bigint,complete:bigint,tariff:bigint,lflid:bigint,additional_user:bigint,pagetype:string,sum:string,msgid:bigint,vol:bigint,icnt:bigint,s:string,did:string,lids:string,pmax:bigint,pmin:bigint,amnt:bigint,lf_dt:bigint,ie:boolean,city:string,serviceid:bigint,at:bigint,pid:bigint,shortcut:bigint,t:string,from_page:string,id:bigint,chid:string,version:bigint,lf_type:string,packageid:bigint,uids:string,ex:bigint,catid:bigint,dpid:bigint,listingfees:string,microcategoryid:bigint,sort:bigint,page:string,delete:boolean,f:string,operationid:bigint,paysysid:bigint,step:bigint,vasid:bigint,lastpaysysid:bigint,onmap:bigint,rooms:string,sgtd:bigint,device_id:string,subscriptionid:bigint,social_id:bigint,push_token:string,chatid:string,locationid:bigint,sortd:string,paystat:bigint,reschid:string,souid:bigint,dupid:bigint,pcid:bigint,banner_type:string,auid:bigint,vas_type:string,abuseid:bigint,tsid:bigint,notification_id:string,cb:string,cy:string,rt:bigint,status:string,isupgrade:boolean,tmid:bigint,sortf:string,ap:boolean,sids:string,answer_text:string,srcp:string,mcids:string,str:string,words:string,dlvritem:bigint,offdelivery:boolean,level:bigint,pillarid:bigint,isdlvr:boolean,extt:bigint,contact_type:string,free_disk_space:bigint,tab:bigint,action:string,dtm:double,abuseids:string,is_seller:boolean,ns_channel:string,ns_type:string,ns_value:boolean,is_user_auth:boolean,duid:bigint,user_key:string,im:boolean,sgtx:string,sgtp:bigint,is_legit:boolean,sgt:string,sgt_mcids:string,objectid:string,objecttype:string,extension:string,geo_lat:double,geo_lng:double,is_antihack_phone_confirm:boolean,antihack_confirm_number:bigint,antihack_user_password_reset:boolean,color:string,exif_model:string,exif_make:string,create_time:bigint,update_time:bigint,app_type:string,ver:string,width:double,height:double,admuserid:bigint,premoderation:boolean,fraud_code_ids:string,afraud_version:bigint,login_type:string,is_first_message:boolean,network_type:string,date:string,plate_number:string,engine_power:string,transport_type:string,transport_category:string,timing:double,timing_duplicate:double,int8erface_version:string,admuser_id:bigint,time_to_content:string,exception_id:string,duplicates:string,abs:string,safedeal:string,dl:boolean,subtype:string,is_verified_inn:boolean,srd:string,form_input_field_name:string,form_input_field_value:string,channels:string,channels_deny:string,target_type:bigint,pmid:bigint,datefrom:string,dateto:string,base_item_id:bigint,reject_wrong_params:string,from_block:bigint,from_position:bigint,service_id:string,amount:string,operation_id:string,isp:boolean,block_items_display:bigint,block_items_added:bigint,is_anon:boolean,search_area:string,retry:boolean,browser_web_push_agreement:boolean,is_auth:boolean,snippet_placement:string,vasid_prev:bigint,_ga:string,advtid:string,sm:boolean,ach:boolean,premiums:bigint,peid:bigint,pdrid:bigint,usedSuggest:boolean,ppeid:bigint,userToken:string,img_id:bigint,vips:bigint,d:boolean,uas:bigint,sr:string,groupId:bigint,launchId:bigint,unique_args:string,model_version:string,subs_price_bonus:double,subs_price_packages:double,subs_price_ext_shop:double,subs_price_total:double,with_shop_flg:boolean,has_cv:boolean,phone_request_flg:boolean,msg_image:string,valid_until:bigint,rating:double,reviews_cnt:bigint,review_id:bigint,share_place:string,user_auth_memo_id:string,call_id:string,is_linked:boolean,rdt:string,notification_message_id:string,adm_comment:string,item_version:bigint,is_adblock:boolean,helpdesk_agent_state:string,anonymous_number_service_responce:string,additional_item:bigint,call_action_type:string,order_cancel_cause:bigint,order_cancel_cause_txt:string,is_verified:boolean,other_phones_number:bigint,item_number:bigint,phone_action:string,is_reverified:boolean,phone_action_type:string,order_cancel_cause_info:string,review_seq_id:bigint,errorsDetailed:string,subs_vertical_ids:string,subs_tariff_id:bigint,subs_proposed_bonus_factor:double,subs_extensions:string,subs_packages:string,caller_user_type:string,message_preview:string,car_doors:string,engine_type:string,transmission:string,car_drive:string,steering_wheel:string,engine_volume:string,article_dissatisfaction_reason:string,redirect_incoming_call_to:string,placement:string,action_type:string,from_source:string,cnt_favourites:bigint,ces_score:bigint,pvas_group_old:bigint,pvas_group:bigint,pvas_groups:string,page_view_session_time:bigint,ctcallid:bigint,is_geo_delivery_widget:boolean,api_path:string,antihack_reason:string,buyer_booking_cancel_cause:string,ticket_comment_id:bigint,src_split_ticket_id:bigint,trgt_split_ticket_id:bigint,time_to_first_byte:bigint,time_to_first_paint8:bigint,time_to_first_int8eractive:bigint,time_to_dom_ready:bigint,time_to_on_load:bigint,time_to_target_content:bigint,time_for_async_target_content:bigint,changed_microcat_id:bigint,scenario:string,cv_use_category:boolean,cv_use_title:boolean,str_buyer_contact_result_reason:string,profile_tab:string,is_original_user_report:boolean,autoteka_user_type:bigint,list_load_number:bigint,ssid:bigint,recommendpaysysid:string,app_version:string,os_version:string,accounts_number:bigint,search_correction_action:string,search_correction_original:string,search_correction_corrected:string,search_correction_method:string,autosort_images:boolean,cnt_subscribers:bigint,is_oasis:boolean,pvas_dates:string,oneclickpayment:boolean,project:string,location_text_input:string,cadastralnumber:string,report_duration:bigint,report_status:boolean,page_number:bigint,deep_link:string,item_add_screen:string,user_ids:string,shop_fraud_reason_ids:string,shop_moderation_action_hash:string,checkbox_an_enable:boolean,message_type:string,app_version_code:string,banner_id:string,shortcut_description:string,close_timeout:bigint,selling_system:string,banner_code:string,wsrc:string,shops_array:string,subscription_promo:boolean,sub_is_ss:boolean,sub_prolong:string,target_page:string,mobile_event_duration:bigint,screen_name:string,content_type:string,mobile_app_page_number:bigint,roads:string,img_download_status:boolean,screen_start_time:bigint,sgt_cat_flag:boolean,ns_owner:string,software_version:string,build:string,adpartner:bigint,ticket_channel:bigint,adslot:string,statid:bigint,current_subs_version:string,new_subs_version:string,subscription_edit:boolean,channel_number:bigint,channels_screen_count:bigint,delivery_help_question:string,banner_due_date:bigint,banner_show_days:boolean,banner_item_id:bigint,chat_error_case:string,has_messages:boolean,search_address_type:string,js_event_type:string,dom_node:string,js_event_slug:string,attr_title:string,attr_link:string,attr_value:string,key_name:string,is_ctrl_pressed:boolean,is_alt_pressed:boolean,is_shift_pressed:boolean,color_theme:string,dom_node_content:string,is_checkbox_checked:boolean,page_x_coord:bigint,page_y_coord:bigint,srd_initial:string,uploaded_files_cnt:bigint,review_additional_info:string,flow_type:bigint,flow_id:string,ces_article:bigint,items_locked_count:bigint,items_not_locked_count:bigint,word_sgt_clicks:bigint,metro:string,msg_app_name:string,msg_request:string,moderation_user_score:double,msg_button_type:string,action_payload:string,msg_chat_list_offset:bigint,ces_hd:bigint,msg_throttling_reason:string,msg_app_version:string,RealtyDevelopment_id:string,metro_list:string,distance_list:string,district_list:string,block_uids:string,msg_blacklist_reason_id:bigint,roads_list:string,msg_random_id:string,msg_int8ernet_connection:boolean,msg_socket_type:bigint,msg_is_push:boolean,cities_list:string,uids_rec:string,email_hash:string,target_hash:string,click_position:bigint,phone_pdhash:string,caller_phone_pdhash:string,avitopro_date_preset:string,skill_id:bigint,safedeal_orderid:bigint,msg_search_query:string,msg_search_success:boolean,msg_chat_page_num:bigint,option_number:bigint,short_term_rent:boolean,issfdl:boolean,helpdesk_user_id:bigint,cv_suggest_show_type:string,review_score:bigint,stage:bigint,sgt_building_id:string,page_from:string,item_condition:string,span_end_time:bigint,custom_param:string,subs_vertical_id:bigint,shop_on_moderation:boolean,parameter_value_slug:string,parameter_value_id:bigint,query_length:bigint,new_category_id:bigint,api_method_name:string,courier_survey_reasons:string,courier_survey_reasons_comment:string,screen_touch_time:bigint,msg_reason_id:bigint,geo_session:string,inactive_page:string,location_suggest_text:string,answer_seq_id:bigint,new_param_ids:string,autoteka_cookie:string,landing_slug:string,autoteka_user_id:string,utm_source:string,utm_medium:string,utm_campaign:string,is_paid:boolean,is_from_avito:boolean,autoteka_order_id:bigint,autoteka_report_id:bigint,safedeal_services:string,performance_timing_redirect_start:double,performance_timing_redirect_end:double,performance_timing_fetch_start:double,performance_timing_domain_lookup_start:double,performance_timing_domain_lookup_end:double,performance_timing_connect_start:double,performance_timing_secure_connection_start:double,performance_timing_connect_end:double,performance_timing_request_start:double,performance_timing_response_start:double,performance_timing_response_end:double,performance_timing_first_paint8:double,performance_timing_first_contentful_paint8:double,performance_timing_dom_int8eractive:double,performance_timing_dom_content_loaded_event_start:double,performance_timing_dom_content_loaded_event_end:double,performance_timing_dom_complete:double,performance_timing_load_event_start:double,performance_timing_load_event_end:double,autoload_tags:string,autoload_not_empty_tags:string,screen_width:bigint,screen_height:bigint,is_new_tab:boolean,autoload_region:string,autoload_subway:string,autoload_street:string,autoload_district:string,autoload_direction_road:string,autoload_distance_to_city:bigint,autoload_item_id:bigint,tip_type:string,error_text:string,abuse_msg:string,cpa_abuse_id:bigint,call_status:string,alid:string,ad_error:bigint,req_num:bigint,app_startup_time:bigint,is_from_ab_test:boolean,upp_call_id:string,upp_provider_id:bigint,upp_virtual_phone:bigint,upp_incoming_phone:string,upp_client:string,upp_linked_phone:string,upp_allocate_id:string,upp_call_eventtype:bigint,upp_call_event_time:bigint,upp_call_is_blocked:boolean,upp_call_duration:bigint,upp_talk_duration:bigint,upp_call_accepted_at:bigint,upp_call_ended_at:bigint,upp_record_url:string,upp_record:boolean,upp_caller_message:string,upp_call_receiver_message:string,upp_transfer_result:string,form_validation_error_texts:string,sgt_item_type:string,landing_action:string,prof_profile_type:string,save_type:bigint,phone_show_result:bigint
        }
        else if (strcmp(argv[i], "--timeout") == 0)
        {
            timeout = atoll(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--brokers") == 0)
        {
            brokers = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--topic") == 0)
        {
            topic = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--group") == 0)
        {
            group = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--task") == 0)
        {
            task = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--filename") == 0)
        {
            filename = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--format") == 0)
        {
            format = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--columns") == 0)
        {
            columns = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--host") == 0)
        {
            host = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--port") == 0)
        {
            port = atol(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--database") == 0)
        {
            database = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--table") == 0)
        {
            table = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--user") == 0)
        {
            user = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--password") == 0)
        {
            password = string(argv[i + 1]);
        }
    }

    Sink *sink = NULL;
    if (format == "csv")
    {
        sink = new CSVSink(columns, "|", "\n");
    }

#ifdef WITH_ORC
    if (format == "orc")
    {
        sink = new ORCSink(columns, filename);
    }
#endif

#ifdef WITH_CH
    if (format == "clickhouse")
    {
        sink = new ClickhouseSink(table, columns, host, port, database, user, password);
    }
#endif

    if (sink == NULL)
    {
        return -1;
    }

    KafkaSource src(brokers, topic, task, group, sink, timeout);
    src.setup();
    src.process();
    src.destroy();
    return 0;
}
