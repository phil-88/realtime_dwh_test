

long double log2_old(long double __x);
#ifdef __i386__
    __asm__(".symver log2_old,log2@GLIBC_2.1");
#elif defined(__amd64__)
    __asm__(".symver log2_old,log2@GLIBC_2.2.5");
#elif defined(__arm__)
    __asm(".symver log2_old,log2@GLIBC_2.4");
#elif defined(__aarch64__)
    __asm__(".symver log2_old,log2@GLIBC_2.17");
#endif

long double __wrap_log2(long double __x)
{
    return log2_old(__x);
}


long double log_old(long double __x);
#ifdef __i386__
    __asm__(".symver log_old,log@GLIBC_2.1");
#elif defined(__amd64__)
    __asm__(".symver log_old,log@GLIBC_2.2.5");
#elif defined(__arm__)
    __asm(".symver log_old,log@GLIBC_2.4");
#elif defined(__aarch64__)
    __asm__(".symver log_old,log@GLIBC_2.17");
#endif

long double __wrap_log(long double __x)
{
    return log_old(__x);
}


double pow_old(double __x, double __y);
#ifdef __i386__
    __asm__(".symver pow_old,pow@GLIBC_2.1");
#elif defined(__amd64__)
    __asm__(".symver pow_old,pow@GLIBC_2.2.5");
#elif defined(__arm__)
    __asm(".symver pow_old,pow@GLIBC_2.4");
#elif defined(__aarch64__)
    __asm__(".symver pow_old,pow@GLIBC_2.17");
#endif

double __wrap_pow(double __x, double __y)
{
    return pow_old(__x, __y);
}



