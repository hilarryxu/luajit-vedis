local ffi = require'ffi'

local ffi_new = ffi.new
local ffi_typeof = ffi.typeof
local ffi_metatype = ffi.metatype
local ffi_str = ffi.string

local clib = ffi.load('vedis')
ffi.cdef[[
static const int VEDIS_OK = 0;

static const int SXRET_OK       = 0;      /* Not an error */
static const int SXERR_MEM      = -1;   /* Out of memory */
static const int SXERR_IO       = -2;   /* IO error */
static const int SXERR_EMPTY    = -3;   /* Empty field */
static const int SXERR_LOCKED   = -4;   /* Locked operation */
static const int SXERR_ORANGE   = -5;   /* Out of range value */
static const int SXERR_NOTFOUND = -6;   /* Item not found */
static const int SXERR_LIMIT    = -7;   /* Limit reached */
static const int SXERR_MORE     = -8;   /* Need more input */
static const int SXERR_INVALID  = -9;   /* Invalid parameter */
static const int SXERR_ABORT    = -10;  /* User callback request an operation abort */
static const int SXERR_EXISTS   = -11;  /* Item exists */
static const int SXERR_SYNTAX   = -12;  /* Syntax error */
static const int SXERR_UNKNOWN  = -13;  /* Unknown error */
static const int SXERR_BUSY     = -14;  /* Busy operation */
static const int SXERR_OVERFLOW = -15;  /* Stack or buffer overflow */
static const int SXERR_WILLBLOCK = -16; /* Operation will block */
static const int SXERR_NOTIMPLEMENTED = -17; /* Operation not implemented */
static const int SXERR_EOF      = -18; /* End of input */
static const int SXERR_PERM     = -19; /* Permission error */
static const int SXERR_NOOP     = -20; /* No-op */
static const int SXERR_FORMAT   = -21; /* Invalid format */
static const int SXERR_NEXT     = -22; /* Not an error */
static const int SXERR_OS       = -23; /* System call return an error */
static const int SXERR_CORRUPT  = -24; /* Corrupted pointer */
static const int SXERR_CONTINUE = -25; /* Not an error: Operation in progress */
static const int SXERR_NOMATCH  = -26; /* No match */
static const int SXERR_RESET    = -27; /* Operation reset */
static const int SXERR_DONE     = -28; /* Not an error */
static const int SXERR_SHORT    = -29; /* Buffer too short */
static const int SXERR_PATH     = -30; /* Path error */
static const int SXERR_TIMEOUT  = -31; /* Timeout */
static const int SXERR_BIG      = -32; /* Too big for processing */
static const int SXERR_RETRY    = -33; /* Retry your call */
static const int SXERR_IGNORE   = -63; /* Ignore */

static const int VEDIS_NOMEM    = SXERR_MEM;     /* Out of memory */
static const int VEDIS_ABORT    = SXERR_ABORT;   /* Another thread have released this instance */
static const int VEDIS_IOERR    = SXERR_IO;      /* IO error */
static const int VEDIS_CORRUPT  = SXERR_CORRUPT; /* Corrupt pointer */
static const int VEDIS_LOCKED   = SXERR_LOCKED;  /* Forbidden Operation */
static const int VEDIS_BUSY	    = SXERR_BUSY;    /* The database file is locked */
static const int VEDIS_DONE	    = SXERR_DONE;    /* Operation done */
static const int VEDIS_PERM     = SXERR_PERM;    /* Permission error */
static const int VEDIS_NOTIMPLEMENTED = SXERR_NOTIMPLEMENTED; /* Method not implemented by the underlying Key/Value storage engine */
static const int VEDIS_NOTFOUND = SXERR_NOTFOUND; /* No such record */
static const int VEDIS_NOOP     = SXERR_NOOP;     /* No such method */
static const int VEDIS_INVALID  = SXERR_INVALID;  /* Invalid parameter */
static const int VEDIS_EOF      = SXERR_EOF;      /* End Of Input */
static const int VEDIS_UNKNOWN  = SXERR_UNKNOWN;  /* Unknown configuration option */
static const int VEDIS_LIMIT    = SXERR_LIMIT;    /* Database limit reached */
static const int VEDIS_EXISTS   = SXERR_EXISTS;   /* Record exists */
static const int VEDIS_EMPTY    = SXERR_EMPTY;    /* Empty record */
static const int VEDIS_FULL     = -73;            /* Full database (unlikely) */
static const int VEDIS_CANTOPEN = -74;            /* Unable to open the database file */
static const int VEDIS_READ_ONLY = -75;           /* Read only Key/Value storage engine */
static const int VEDIS_LOCKERR  = -76;            /* Locking protocol error */

static const int VEDIS_CONFIG_ERR_LOG             = 1;  /* TWO ARGUMENTS: const char **pzBuf, int *pLen */
static const int VEDIS_CONFIG_MAX_PAGE_CACHE      = 2;  /* ONE ARGUMENT: int nMaxPage */
static const int VEDIS_CONFIG_KV_ENGINE           = 4;  /* ONE ARGUMENT: const char *zKvName */
static const int VEDIS_CONFIG_DISABLE_AUTO_COMMIT = 5;  /* NO ARGUMENTS */
static const int VEDIS_CONFIG_GET_KV_NAME         = 6;  /* ONE ARGUMENT: const char **pzPtr */
static const int VEDIS_CONFIG_DUP_EXEC_VALUE      = 7;  /* ONE ARGUMENT: vedis_value **ppOut */
static const int VEDIS_CONFIG_RELEASE_DUP_VALUE   = 8;  /* ONE ARGUMENT: vedis_value *pIn */
static const int VEDIS_CONFIG_OUTPUT_CONSUMER     = 9;  /* TWO ARGUMENTS: int (*xConsumer)(vedis_value *pOut,void *pUserdata), void *pUserdata */

typedef struct vedis vedis;
typedef struct vedis_value vedis_value;

const char *vedis_lib_version();

int vedis_open(vedis **ppStore, const char *zStorage);
int vedis_close(vedis *pStore);

int vedis_kv_store(vedis *pStore,const void *pKey,int nKeyLen,const void *pData,int64_t nDataLen);
int vedis_kv_store_fmt(vedis *pStore,const void *pKey,int nKeyLen,const char *zFormat,...);
int vedis_kv_fetch(
      vedis *pStore,
      const void *pKey,int nKeyLen,
      void *pBuf,
      int64_t *pSize /* IN: Buffer Size / OUT: Record Data Size */
);
int vedis_kv_append(vedis *pStore,const void *pKey,int nKeyLen,const void *pData,int64_t nDataLen);
int vedis_kv_append_fmt(vedis *pStore,const void *pKey,int nKeyLen,const char *zFormat,...);
int vedis_kv_delete(vedis *pStore,const void *pKey,int nKeyLen);

int vedis_exec(vedis *pStore,const char *zCmd,int nLen);
int vedis_exec_fmt(vedis *pStore,const char *zFmt,...);
int vedis_exec_result(vedis *pStore,vedis_value **ppOut);

int vedis_begin(vedis *pStore);
int vedis_commit(vedis *pStore);
int vedis_rollback(vedis *pStore);

int vedis_util_random_string(vedis *pStore,char *zBuf,unsigned int buf_size);
unsigned int vedis_util_random_num(vedis *pStore);

int vedis_config(vedis *pStore,int nOp,...);

int vedis_value_to_int(vedis_value *pValue);
int vedis_value_to_bool(vedis_value *pValue);
int64_t vedis_value_to_int64(vedis_value *pValue);
double vedis_value_to_double(vedis_value *pValue);
const char * vedis_value_to_string(vedis_value *pValue,int *pLen);

int vedis_value_is_int(vedis_value *pVal);
int vedis_value_is_float(vedis_value *pVal);
int vedis_value_is_bool(vedis_value *pVal);
int vedis_value_is_string(vedis_value *pVal);
int vedis_value_is_null(vedis_value *pVal);
int vedis_value_is_numeric(vedis_value *pVal);
int vedis_value_is_scalar(vedis_value *pVal);
int vedis_value_is_array(vedis_value *pVal);

int vedis_array_reset(vedis_value *pArray);
vedis_value * vedis_array_next_elem(vedis_value *pArray);
unsigned int vedis_array_count(vedis_value *pArray);

typedef struct {
  void *db;
} vedis_handle_t;

typedef struct {
  void *db;
  void *cur;
} vedis_cursor_t;
]]

local error_codes = {}

local _M = {
  C = clib,
  E = error_codes
}
local vedis_mt = {
  __index = _M
}

local ct_db_ptr = ffi_typeof('vedis*[1]')

local function handle_error(rc)
  if rc == clib.VEDIS_OK then
    return true
  end
  return nil, error_codes[rc] or rc
end

--------------------------------
-- vedis_handle_t
--------------------------------
local vedis_handle_t = ffi_typeof('vedis_handle_t')
local vedis_handle_t_mt = {
  __new = function(ct, handle)
    return ffi_new(ct, handle)
  end,
  __gc = function(self)
    if self.db == nil then return end
    clib.vedis_close(self.db)
    self.db = nil
  end
}
ffi_metatype(vedis_handle_t, vedis_handle_t_mt)

--------------------------------
-- vedis_value
--------------------------------
local vedis_value = ffi_typeof('vedis_value')
local vedis_value_meths = {
  to_string = function(self)
    local p_len = ffi_new('int[1]')
    local s = clib.vedis_value_to_string(self, p_len)
    return ffi_str(s, p_len[0])
  end,
  to_int = function(self)
    return clib.vedis_value_to_int(self)
  end,
  to_bool = function(self)
    return clib.vedis_value_to_bool(self) == 1
  end,
  to_double = function(self)
    return clib.vedis_value_to_double(self) == 1
  end,
  to_int64 = function(self)
    return clib.vedis_value_to_int64(self)
  end,

  is_int = function(self)
    return clib.vedis_value_is_int(self) == 1
  end,
  is_float = function(self)
    return clib.vedis_value_is_float(self) == 1
  end,
  is_bool = function(self)
    return clib.vedis_value_is_bool(self) == 1
  end,
  is_string = function(self)
    return clib.vedis_value_is_string(self) == 1
  end,
  is_null = function(self)
    return clib.vedis_value_is_null(self) == 1
  end,
  is_numeric = function(self)
    return clib.vedis_value_is_numeric(self) == 1
  end,
  is_scalar = function(self)
    return clib.vedis_value_is_scalar(self) == 1
  end,
  is_array = function(self)
    return clib.vedis_value_is_array(self) == 1
  end,

  array_next_elem = function(self)
    return clib.vedis_array_next_elem(self)
  end,
  array_reset = function(self)
    return clib.vedis_array_reset(self)
  end,
  array_count = function(self)
    return clib.vedis_array_count(self)
  end
}
local vedis_value_mt = {
  __index = vedis_value_meths
}
ffi_metatype(vedis_value, vedis_value_mt)

--------------------------------
-- vedis
--------------------------------
function _M.lib_version()
  return ffi_str(clib.vedis_lib_version())
end

function _M.open(filename)
  local p_db = ffi_new('vedis*[1]')
  local rc = clib.vedis_open(p_db, filename or ':mem:')
  if rc ~= clib.VEDIS_OK then
    return nil
  end

  local handle = vedis_handle_t(p_db[0])
  local self = {
    db = handle.db,
    handle = handle
  }
  return setmetatable(self, vedis_mt)
end

function _M:close()
  local rc = clib.vedis_close(self.db)
  if rc == clib.VEDIS_OK then
    self.handle.db = nil
    self.handle = nil
    self.db = nil
  end
  return handle_error(rc)
end

function _M:set(key, value)
  local rc = clib.vedis_kv_store(self.db, key, #key, value, #value)
  return handle_error(rc)
end

function _M:append(key, value)
  local rc = clib.vedis_kv_append(self.db, key, #key, value, #value)
  return handle_error(rc)
end

function _M:get(key, value_len)
  local buf = ffi_new('unsigned char[?]', len)
  local p_len = ffi_new('int64_t[1]')
  p_len[0] = value_len
  local rc = clib.vedis_kv_fetch(self.db, key, #key, buf, p_len)
  if rc ~= clib.VEDIS_OK then
    return nil, rc
  end
  return ffi_str(buf, p_len[0])
end

function _M:delete(key)
  local rc = clib.vedis_kv_delete(self.db, key, #key)
  if rc == clib.VEDIS_NOTFOUND then return true end
  return handle_error(rc)
end

function _M:exec(cmd)
  local rc = clib.vedis_exec(self.db, cmd, #cmd)
  return handle_error(rc)
end

function _M:exec_result(cmd)
  local value_ptr = ffi_new('vedis_value*[1]')
  local rc = clib.vedis_exec_result(self.db, value_ptr)
  if rc ~= clib.VEDIS_OK then
    return nil, rc
  end
  return value_ptr[0]
end

function _M:begin()
  local rc = clib.vedis_begin(self.db)
  return handle_error(rc)
end

function _M:commit()
  local rc = clib.vedis_commit(self.db)
  return handle_error(rc)
end

function _M:rollback()
  local rc = clib.vedis_rollback(self.db)
  return handle_error(rc)
end

function _M:random_string(len)
  local buf = ffi_new('char[?]', len + 1)
  clib.vedis_util_random_string(self.db, buf, len)
  return ffi_str(buf, len)
end

function _M:random_num()
  return clib.vedis_util_random_num(self.db)
end

return _M
