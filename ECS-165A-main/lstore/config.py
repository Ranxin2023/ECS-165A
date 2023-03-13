PAGE_SIZE = 4096
MAX_PAGE = 16
RECORD_SIZE = 8
RECORD_PER_PAGE = PAGE_SIZE // RECORD_SIZE # 512 records per page
RECORD_PER_PAGERANGE =  MAX_PAGE * RECORD_PER_PAGE # 8192
DEFAULT_PAGE = 5
MAX_INT = 2**64 - 1
BUFFER_POOL_SIZE = 1000

# [rid, int(time), schema_encoding, indirection]
RID = 0
TIME = 1
SCHEMA_ENCODING = 2
INDIRECTION = 3
BASEID = 4


