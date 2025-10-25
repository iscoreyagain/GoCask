package internal

import "time"

const MaxActiveFileSize = 128 * 1024 * 1024 //128MB
const logEntryHeaderSize = 21               // 4 + 8 + 4 + 4 + 1
const syncInterval = 1 * time.Second
