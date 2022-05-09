local ffi = require "ffi"
local string = require "string"

local ffi_new = ffi.new
local ffi_str = ffi.string
local ffi_cast = ffi.cast
local str_fmt = string.format

local mod = {}
local aux = {}

local is_luajit = pcall(require, "jit")
local bind_args
local clib

local load_clib, bind_clib -- Forward declaration

local function init(mod, name_or_args)
  if clib ~= nil then
    return mod
  end

  if type(name_or_args) == "table" then
    bind_args = name_or_args
    bind_args.name = bind_args.name or bind_args[1]
  elseif type(name_or_args) == "string" then
    bind_args = {}
    bind_args.name = name_or_args
  end

  clib = load_clib()
  bind_clib()

  return mod
end

function load_clib()
  if bind_args.clib ~= nil then
    return bind_args.clib
  end

  if type(bind_args.name) == "string" then
    if type(bind_args.path) == "string" then
      return ffi.load(package.searchpath(bind_args.name, bind_args.path))
    else
      return ffi.load(bind_args.name)
    end
  end

  -- If no library or name is provided, we just
  -- assume that the appropriate libraries
  -- are statically linked to the calling program
  return ffi.C
end

function bind_clib()
  -----------------------------------------------------------
  --  Namespaces
  -----------------------------------------------------------
  local consts = {} -- Table for contants
  local funcs = {} -- Table for functions
  local types = {} -- Table for types
  local cbs = {} -- Table for callbacks

  mod.consts = consts
  mod.funcs = funcs
  mod.types = types
  mod.cbs = cbs
  mod.clib = clib

  -- Access to funcs from module namespace by default
  aux.set_mt_method(mod, "__index", funcs)

  -----------------------------------------------------------
  --  Constants
  -----------------------------------------------------------
  consts.VEDIS_OK = 0

  -- Result Codes
  consts.SXRET_OK = 0 -- /* Not an error */
  consts.SXERR_MEM = -1 -- /* Out of memory */
  consts.SXERR_IO = -2 -- /* IO error */
  consts.SXERR_EMPTY = -3 -- /* Empty field */
  consts.SXERR_LOCKED = -4 -- /* Locked operation */
  consts.SXERR_ORANGE = -5 -- /* Out of range value */
  consts.SXERR_NOTFOUND = -6 -- /* Item not found */
  consts.SXERR_LIMIT = -7 -- /* Limit reached */
  consts.SXERR_MORE = -8 -- /* Need more input */
  consts.SXERR_INVALID = -9 -- /* Invalid parameter */
  consts.SXERR_ABORT = -10 -- /* User callback request an operation abort */
  consts.SXERR_EXISTS = -11 -- /* Item exists */
  consts.SXERR_SYNTAX = -12 -- /* Syntax error */
  consts.SXERR_UNKNOWN = -13 -- /* Unknown error */
  consts.SXERR_BUSY = -14 -- /* Busy operation */
  consts.SXERR_OVERFLOW = -15 -- /* Stack or buffer overflow */
  consts.SXERR_WILLBLOCK = -16 -- /* Operation will block */
  consts.SXERR_NOTIMPLEMENTED = -17 -- /* Operation not implemented */
  consts.SXERR_EOF = -18 -- /* End of input */
  consts.SXERR_PERM = -19 -- /* Permission error */
  consts.SXERR_NOOP = -20 -- /* No-op */
  consts.SXERR_FORMAT = -21 -- /* Invalid format */
  consts.SXERR_NEXT = -22 -- /* Not an error */
  consts.SXERR_OS = -23 -- /* System call return an error */
  consts.SXERR_CORRUPT = -24 -- /* Corrupted pointer */
  consts.SXERR_CONTINUE = -25 -- /* Not an error: Operation in progress */
  consts.SXERR_NOMATCH = -26 -- /* No match */
  consts.SXERR_RESET = -27 -- /* Operation reset */
  consts.SXERR_DONE = -28 -- /* Not an error */
  consts.SXERR_SHORT = -29 -- /* Buffer too short */
  consts.SXERR_PATH = -30 -- /* Path error */
  consts.SXERR_TIMEOUT = -31 -- /* Timeout */
  consts.SXERR_BIG = -32 -- /* Too big for processing */
  consts.SXERR_RETRY = -33 -- /* Retry your call */
  consts.SXERR_IGNORE = -63 -- /* Ignore */

  consts.VEDIS_NOMEM = consts.SXERR_MEM -- /* Out of memory */
  consts.VEDIS_ABORT = consts.SXERR_ABORT -- /* Another thread have released this instance */
  consts.VEDIS_IOERR = consts.SXERR_IO -- /* IO error */
  consts.VEDIS_CORRUPT = consts.SXERR_CORRUPT -- /* Corrupt pointer */
  consts.VEDIS_LOCKED = consts.SXERR_LOCKED -- /* Forbidden Operation */
  consts.VEDIS_BUSY = consts.SXERR_BUSY -- /* The database file is locked */
  consts.VEDIS_DONE = consts.SXERR_DONE -- /* Operation done */
  consts.VEDIS_PERM = consts.SXERR_PERM -- /* Permission error */
  consts.VEDIS_NOTIMPLEMENTED = consts.SXERR_NOTIMPLEMENTED -- /* Method not implemented by the underlying Key/Value storage engine */
  consts.VEDIS_NOTFOUND = consts.SXERR_NOTFOUND -- /* No such record */
  consts.VEDIS_NOOP = consts.SXERR_NOOP -- /* No such method */
  consts.VEDIS_INVALID = consts.SXERR_INVALID -- /* Invalid parameter */
  consts.VEDIS_EOF = consts.SXERR_EOF -- /* End Of Input */
  consts.VEDIS_UNKNOWN = consts.SXERR_UNKNOWN -- /* Unknown configuration option */
  consts.VEDIS_LIMIT = consts.SXERR_LIMIT -- /* Database limit reached */
  consts.VEDIS_EXISTS = consts.SXERR_EXISTS -- /* Record exists */
  consts.VEDIS_EMPTY = consts.SXERR_EMPTY -- /* Empty record */
  consts.VEDIS_FULL = -73 -- /* Full database (unlikely) */
  consts.VEDIS_CANTOPEN = -74 -- /* Unable to open the database file */
  consts.VEDIS_READ_ONLY = -75 -- /* Read only Key/Value storage engine */
  consts.VEDIS_LOCKERR = -76 -- /* Locking protocol error */

  -- For C pointers comparison
  if not is_luajit then
    consts.NULL = ffi.C.NULL
  end

  -----------------------------------------------------------
  --  Types
  -----------------------------------------------------------
  ffi.cdef [[
    typedef struct vedis vedis;
    typedef struct vedis_value vedis_value;
  ]]

  local vedis_mt = aux.class()
  local vedis_value_mt = aux.class()

  -----------------------------------------------------------
  --  Functions
  -----------------------------------------------------------
  local function handle_error(rc)
    if rc == consts.VEDIS_OK then
      return true
    end

    return nil, rc
  end

  ffi.cdef [[
    const char *vedis_lib_version();

    int vedis_open(vedis **ppStore, const char *zStorage);
    int vedis_close(vedis *pStore);
  ]]

  function funcs.lib_version()
    return aux.wrap_string(clib.vedis_lib_version())
  end

  function funcs.open(filename)
    local p_store = ffi_new "vedis*[1]"
    local rc = clib.vedis_open(p_store, filename or ":mem:")
    if rc ~= consts.VEDIS_OK then
      return nil, rc
    end

    return p_store[0]
  end

  function funcs.close(db)
    local rc = clib.vedis_close(db)
    return handle_error(rc)
  end

  ffi.cdef [[
    int vedis_kv_store(vedis *pStore,const void *pKey,int nKeyLen,const void *pData,int64_t nDataLen);
    int vedis_kv_fetch(
          vedis *pStore,
          const void *pKey,int nKeyLen,
          void *pBuf,
          int64_t *pSize /* IN: Buffer Size / OUT: Record Data Size */
    );
    int vedis_kv_append(vedis *pStore,const void *pKey,int nKeyLen,const void *pData,int64_t nDataLen);
    int vedis_kv_delete(vedis *pStore,const void *pKey,int nKeyLen);
  ]]

  function funcs.get(db, key, value_len)
    local buf = ffi_new("unsigned char[?]", value_len)
    local p_len = ffi_new "int64_t[1]"
    p_len[0] = value_len

    local rc = clib.vedis_kv_fetch(db, key, #key, buf, p_len)
    if rc ~= consts.VEDIS_OK then
      return handle_error(rc)
    end

    return ffi_str(buf, p_len[0])
  end

  function funcs.set(db, key, value)
    local rc = clib.vedis_kv_store(db, key, #key, value, #value)
    return handle_error(rc)
  end

  function funcs.delete(db, key)
    local rc = clib.vedis_kv_delete(db, key, #key)
    if rc == consts.VEDIS_NOTFOUND then
      return false
    end
    return handle_error(rc)
  end

  -----------------------------------------------------------
  --  Extended Functions
  -----------------------------------------------------------
  vedis_mt.close = funcs.close

  -----------------------------------------------------------
  --  Finalize types metatables
  -----------------------------------------------------------
  ffi.metatype("vedis", vedis_mt)
end

-----------------------------------------------------------
--  Auxiliary
-----------------------------------------------------------
function aux.class()
  local class = {}
  class.__index = class
  return class
end

function aux.set_mt_method(t, k, v)
  local mt = getmetatable(t)
  if mt then
    mt[k] = v
  else
    setmetatable(t, { [k] = v })
  end
end

if is_luajit then
  -- LuaJIT way to compare with NULL
  function aux.is_null(ptr)
    return ptr == nil
  end
else
  -- LuaFFI way to compare with NULL
  function aux.is_null(ptr)
    return ptr == ffi.C.NULL
  end
end

function aux.wrap_string(cstr)
  if not aux.is_null(cstr) then
    return ffi_str(cstr)
  end
  return nil
end

function aux.wrap_bool(c_bool)
  return c_bool ~= 0
end

-- mod
return setmetatable(mod, { __call = init })
