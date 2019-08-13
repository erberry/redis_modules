#include "redismodule.h"
#include <stdlib.h>

int ExtendHMSET_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
   if (argc < 4) return RedisModule_WrongArity(ctx);

    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);

    int key_t = RedisModule_KeyType(key);

    mstime_t expire = 0;
    int nextc = 2;

    size_t len;
    const char* x = RedisModule_StringPtrLen(argv[2], &len);

    if (len == 2 && (x[0] == 'E' || x[0] == 'e') && (x[1] == 'X' || x[1] == 'x')) {
        if (RedisModule_StringToLongLong(argv[3],&expire) != REDISMODULE_OK)
            return RedisModule_ReplyWithError(ctx,"ERR invalid expire time");
        nextc += 2;
    }

    x = RedisModule_StringPtrLen(argv[nextc], &len);
    if (len == 2 && (x[0] == 'N' || x[0] == 'n') && (x[1] == 'X' || x[1] == 'x')) {
        nextc += 1;
        if (key_t != REDISMODULE_KEYTYPE_EMPTY)
            return RedisModule_ReplyWithNull(ctx);
    } else if (len == 2 && (x[0] == 'X' || x[0] == 'x') && (x[1] == 'X' || x[1] == 'x')){
        nextc += 1;
        if (key_t == REDISMODULE_KEYTYPE_LIST)
            return RedisModule_ReplyWithNull(ctx);
    }

    if ((argc - nextc)%2 == 1) {
        return RedisModule_ReplyWithError(ctx, "wrong number of arguments for HMSET");
    }

    int i;
    for (i = nextc; i<argc; i+=2) {
        RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[i], argv[i+1], NULL);
    }

    if (expire != 0)
        RedisModule_SetExpire(key, expire);

    RedisModule_ReplyWithSimpleString(ctx,"OK");
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    if (RedisModule_Init(ctx,"extend",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"extend.hmset",
        ExtendHMSET_RedisCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}