// -*-mode:c++; coding:utf-8-*-

#ifndef _GFS_HPP_
#define _GFS_HPP_

#include <string>
#include <cassert>

#include <sys/uio.h>		// for iovec

#include <gfs_client/file_system.h>
#include <gfs_client/file.h>
#include <gfs_client/file_info.h>
#include <gfs_client/directory.h>
#include <gfs_client/limit.h>
#include <gfs_client/gfs_errno.h>
#include <gfs_client/file_status.h>

// 关于出错重试的宏定义
#ifdef GFS_RETRY_DISABLED

#	define RETRY_DO do
#	define RETRY_LOG(msg)
#	define RETRY_ON(cond) while(false)

#else

#	include <unistd.h>		// for usleep

#ifndef RETRY_HOOK
#       define RETRY_HOOK(errno, couter) // nothing
#endif

#	define RETRY_DO std::size_t retry_counter = 0; do
#	define RETRY_LOG(msg) do {					\
		if(retry_counter != 0)					\
		{							\
                        RETRY_HOOK(get_errno(), retry_counter);         \
			retry_interval_sleep();				\
		}							\
	}while(false)
#	define RETRY_ON(cond) while((cond) && can_retry(retry_counter ++))

#endif	// GFS_RETRY_DISABLED

                
namespace gfs
{

// 定义重试需要的值和函数

#ifndef GFS_RETRY_DISABLED
		
#ifndef GFS_RETRY_TIMES
#	define GFS_RETRY_TIMES 30
#endif  // GFS_RETRY_TIMES
		
#ifndef GFS_RETRY_INTERVAL
#	define GFS_RETRY_INTERVAL 9
#endif  // GFS_RETRY_INTERVAL

static const std::size_t retry_times = GFS_RETRY_TIMES;
static const std::size_t retry_interval = GFS_RETRY_INTERVAL; // seconds

inline
bool can_retry(std::size_t i) {
        return i < retry_times;
}

// 由于gfs内部有重试、sleep机制，所以这里就不需要sleep了
inline
void retry_interval_sleep() {
        ::sleep(retry_interval);
}

#endif	// GFS_RETRY_DISABLED

typedef FileSystem filesystem_type;

// init the gfs
inline
void init(const char *p_conf) {
        filesystem_type * const gfs = filesystem_type::get(p_conf);
        assert(gfs != NULL);
        (void)gfs;
}

inline
filesystem_type *file_system() {
        return filesystem_type::get();
}

inline
int get_errno() {
        return gfs_errno;
}

inline
void set_errno(int no) {
        gfs_errno = no;
}

typedef File* file_t;
typedef int64_t ssize_t;
typedef uint64_t size_t;
typedef int64_t offset_t; // it's signed
typedef struct ::iovec iovec_t;
typedef Directory * dir_t;

enum {
        MAX_IOVEC_LEN = 64,
        MAX_FILENAME_LEN = ((MAX_PATH_LENGTH > 512)
                            ? MAX_PATH_LENGTH
                            : 512)
};

inline
void iovec_init(iovec_t &iov,
                void *data,
                size_t size) {
        iov.iov_base = data;
        iov.iov_len = size;
}

static const file_t BAD_FILE = NULL;
static const offset_t BAD_OFFSET = -1LL;

enum seek_type
{
        ST_SEEK_SET = SEEK_SET,
        ST_SEEK_CUR = SEEK_CUR,
        ST_SEEK_END = SEEK_END
};
typedef seek_type seek_t;

enum mode_type
{
        MT_O_RDONLY = O_RDONLY,
        MT_O_WRONLY = O_WRONLY,
        MT_O_RDWR = O_RDWR,
				
        MT_O_APPEND = O_APPEND,
        MT_O_CREATE = O_CREAT,
        MT_O_TRUNC = O_TRUNC
        // ...
};
typedef mode_type mode_t;

typedef ::FileStatus file_status;

inline
size_t get_size(const file_status &p_status) {
        return p_status.get_len();
}

inline
bool is_directory(const file_status &p_status) {
        return p_status.is_dir();
}

inline
bool is_regular(const file_status &p_status) {
        return p_status.is_dir() == false;
}
		
struct file_info
{
        std::string m_name; // 文件名称，不包含路径
        bool m_is_dir;
};

inline
std::string get_name(const file_info &p_info) {
        return p_info.m_name;
}

inline
bool is_directory(const file_info &p_info) {
        return p_info.m_is_dir;
}

inline
bool is_regular(const file_info &p_info) {
        return p_info.m_is_dir == false;
}

inline
bool exists(const char *p_path) {
        int32_t ret = 0;
        RETRY_DO {
                RETRY_LOG("gfs::exists failed");
                ret = file_system()->exists(p_path);
        } RETRY_ON ((ret != 0) && (ret != 1));
        // TODO: 可能这里无限重试更好？
        return ret == 1;
}

inline
bool close(file_t p_file) {
        return file_system()->close(p_file) == 0;
}

inline
file_t open(const char *p_path,
            mode_t p_mode = MT_O_RDONLY) {
        file_t fd = BAD_FILE;
        RETRY_DO {
                RETRY_LOG("gfs::open failed");
                fd = file_system()->open(p_path,
                                         static_cast<int32_t>(p_mode));
        } RETRY_ON ((fd == BAD_FILE)
                    && (ERR_EXIST != get_errno())); // 文件已存在错误则不重试
        return fd;
}

inline
file_t open(const char *p_path,
            mode_t p_mode = MT_O_RDONLY,
            std::size_t replica_number) {
        file_t fd = BAD_FILE;
        RETRY_DO {
                RETRY_LOG("gfs::open failed");
                fd = file_system()->open(p_path,
                                         static_cast<int32_t>(p_mode),
                                         static_cast<int32_t>(replica_number));
        } RETRY_ON ((fd == BAD_FILE)
                    && (ERR_EXIST != get_errno())); // 文件已存在错误则不重试
        return fd;
}

inline
file_t create(const char *p_path) {
        file_t fd = BAD_FILE;
        RETRY_DO {
                RETRY_LOG("gfs::create failed");
                if(fd != BAD_FILE)
                {
                        close(fd);
                }
                fd = file_system()->creat(p_path);
        } RETRY_ON ((fd == BAD_FILE) &&
                    (! exists(p_path)));
        // 创建成功后，验证文件是否存在；因为发生过创建
        // 成功后，文件不存在的现象。
        return fd;
}

inline
file_t create(const char *p_path,
              std::size_t replica_number) {
        file_t fd = BAD_FILE;
        RETRY_DO {
                RETRY_LOG("gfs::create failed");
                if(fd != BAD_FILE)
                {
                        close(fd);
                }
                fd = file_system()->creat(p_path,
                                          static_cast<int32_t>(replica_number));
        } RETRY_ON ((fd == BAD_FILE) &&
                    (! exists(p_path)));
        // 创建成功后，验证文件是否存在；因为发生过创建
        // 成功后，文件不存在的现象。
        return fd;
}

inline
ssize_t read(file_t p_file,
             void *p_buffer,
             size_t p_count) {
        ssize_t ret = 0;
        RETRY_DO {
                RETRY_LOG("gfs::read failed");
                ret = p_file->read(p_buffer, p_count);
        } RETRY_ON ((ret == -1LL)
                    && (ERR_NODE_NOEXIST != get_errno()));
        return ret;
}

inline
offset_t append(file_t p_file,
                const void *p_buffer,
                size_t p_count) {
        offset_t ret = 0;
        RETRY_DO {
                RETRY_LOG("gfs::append failed");
                ret = p_file->append(p_buffer, p_count);
        } RETRY_ON(ret < 0);
        return ret;
}

inline
ssize_t write(file_t p_file,
              const void *p_buffer,
              size_t p_count) {
        ssize_t ret = 0;
        RETRY_DO {
                RETRY_LOG("gfs::write failed");
                ret = p_file->write(p_buffer, p_count);
        } RETRY_ON((ret < 0)
                   && (ERR_NODE_NOEXIST != get_errno()));
        return ret;
}
		
inline
ssize_t writev(file_t p_file,
               const iovec_t *p_iov,
               size_t p_count) {
        ssize_t ret = 0;
        RETRY_DO {
                RETRY_LOG("gfs::write failed");
                ret = p_file->writev(p_iov, p_count);
        } RETRY_ON((ret < 0)
                   && (ERR_NODE_NOEXIST != get_errno()));
        return ret;
}
		

inline
offset_t seek(file_t p_file,
              offset_t p_offset,
              seek_t p_whence) {
        offset_t ret = 0;
        RETRY_DO {
                RETRY_LOG("gfs::seek failed");
                ret = p_file->lseek(p_offset,
                                    static_cast<int32_t>(p_whence));
        } RETRY_ON((ret < 0)
                   && (ERR_NODE_NOEXIST != get_errno()));
        return ret;
}

inline
bool remove(const char *p_path) {
        int32_t ret = 0;
        RETRY_DO {
                RETRY_LOG("gfs::remove failed");
                ret = file_system()->unlink(p_path);
        } RETRY_ON(ret < 0);
        return ret == 0;
}

inline
bool rename(const char *p_old_path,
            const char *p_new_path) {
        int32_t ret = 0;
        RETRY_DO {
                RETRY_LOG("gfs::rename failed");
                ret = file_system()->rename(p_old_path,
                                            p_new_path);
        } RETRY_ON(ret < 0);
        return ret == 0;
}

inline
bool stat(file_status &p_status,
          const char *p_path) {
        uint32_t ret = 0;
        RETRY_DO {
                RETRY_LOG("gfs::stat failed");
                ret = file_system()->stat(p_path, &p_status, true/*get_exact_len*/);
        } RETRY_ON((ret < 0) &&
                   (get_errno() != ERR_NODE_NOEXIST));
        return ret == 0;
}

inline
bool mkdir(const char *p_path) {
        int32_t ret = 0;
        RETRY_DO {
                RETRY_LOG("gfs::mkdir failed");
                ret = file_system()->mkdir(p_path);
        } RETRY_ON((ret < 0) && (! exists(p_path)));
        return ret == 0;
}

//
// is_regular, is_directory: stat返回false，即认为文件
// 不存在，此时返回false即可。
//
// 即语义为：返回false表示文件不存在或不满足属性要求
// 

inline
bool is_regular(const char *p_path) {
        file_status _status;
        return stat(_status, p_path) &&
                is_regular(_status);
}

inline
bool is_directory(const char *p_path) {
        file_status _status;
        return stat(_status, p_path) &&
                is_directory(_status);
}

// 这里列出的是目录下的文件名，不包含路径的
// FileInfoContainer - fs::file_info container,
//                     and has push_back() method
template<typename FileInfoContainer>
inline
bool list_files(FileInfoContainer &p_infos,
                const char *p_path) {
        dir_t _dir = NULL;
        RETRY_DO {
                RETRY_LOG("gfs::list_files failed");
                _dir = file_system()->opendir(p_path);
        } RETRY_ON((_dir == NULL) && is_directory(p_path));

        if (_dir == NULL)
                return false;

        // 注意，如果有. ..的话，要过滤掉
        Directory::iterator _iter;
        file_info _info;
        while(!_dir->done())
        {
                _iter = _dir->next();
                _info.m_name = _iter->name();
                _info.m_is_dir = _iter->is_dir();
                p_infos.push_back(_info);
        }

        file_system()->closedir(_dir);
        return true;
}

//
// readn, writen 返回值小于p_count表示出错，即为-1或已
// 经读出或写入的数据长度；成功时返回值等于p_count
//
inline
ssize_t readn(file_t p_file,
              void *p_buffer,
              size_t p_count) {
        size_t _readed = 0;
        char *_pos = static_cast<char*>(p_buffer);
        ssize_t _ret = -1;
        while(_readed < p_count) {
                _ret = read(p_file, _pos, p_count - _readed);
                if (_ret < 0) {
                        return ((_readed == 0) ? ssize_t(-1) : ssize_t(_readed));
                } else if (_ret == 0) {
                        return _readed;
                } else {
                        _readed += _ret;
                        _pos += _ret;
                }
        }
        return _readed;
}

inline
ssize_t writen(file_t p_file,
               const void *p_buffer,
               size_t p_count) {
        size_t _writen = 0;
        const char *_pos = static_cast<const char*>(p_buffer);
        while(_writen < p_count) {
                ssize_t _ret = write(p_file, _pos, p_count - _writen);
                if (_ret < 0) {
                        return ((_writen == 0) ? ssize_t(-1) : ssize_t(_writen)); // -1 or writen len
                } else if (_ret == 0) {
                        return _writen;
                } else {
                        _writen += _ret;
                        _pos += _ret;
                }
        }
        return _writen;
}

//
// pread, preadn, pwrite, pwriten 操作，不更新文件指针（偏移量）
// 

inline
ssize_t pread(file_t p_file,
              void *p_buffer,
              size_t p_count,
              offset_t p_offset) {
        offset_t _org = seek(p_file, p_offset, ST_SEEK_SET);
        ssize_t _ret = read(p_file, p_buffer, p_count);
        seek(p_file, _org, ST_SEEK_SET);
        return _ret;
}

inline
ssize_t pwrite(file_t p_file,
               const void *p_buffer,
               size_t p_count,
               offset_t p_offset) {
        offset_t _org = seek(p_file, p_offset, ST_SEEK_SET);
        ssize_t _ret = write(p_file, p_buffer, p_count);
        seek(p_file, _org, ST_SEEK_SET);
        return _ret;
}

inline
ssize_t preadn(file_t p_file,
               void *p_buffer,
               size_t p_count,
               offset_t p_offset) {
        offset_t _org = seek(p_file, p_offset, ST_SEEK_SET);
        ssize_t _ret = readn(p_file, p_buffer, p_count);
        seek(p_file, _org, ST_SEEK_SET);
        return _ret;
}

inline
ssize_t pwriten(file_t p_file,
                const void *p_buffer,
                size_t p_count,
                offset_t p_offset) {
        offset_t _org = seek(p_file, p_offset, ST_SEEK_SET);
        ssize_t _ret = writen(p_file, p_buffer, p_count);
        seek(p_file, _org, ST_SEEK_SET);
        return _ret;
}

} // namespace gfs

#undef RETRY_DO
#undef RETRY_LOG
#undef RETRY_ON

#endif	// _LOCAL_FS_HPP_
