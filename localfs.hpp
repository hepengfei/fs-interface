// -*-mode:c++; coding:gb18030-*-

#ifndef _LOCALFS_HPP_
#define _LOCALFS_HPP_


#include <cstdio>
#include <string>
#include <vector>

//// make sure compile with
////  -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
//// options
//#ifndef _LARGEFILE_SOURCE
//#error "must use -D_LARGEFILE_SOURCE option"
//#endif
//
//#ifndef _FILE_OFFSET_BITS
//#error "must use -D_FILE_OFFSET_BITS option"
//#endif

#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/uio.h>		// for iovec
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>

namespace localfs
{

        
// empty init
inline
void init() {
        return;
}
inline
void init(const char *) {
        return;
}

inline
int get_errno() {
        return errno;
}

inline
void set_errno(int no) {
        errno = no;
}

        
typedef int file_t;
typedef int64_t ssize_t;
typedef uint64_t size_t;
typedef ::off_t offset_t; // it's signed
typedef struct ::iovec iovec_t;
typedef DIR * dir_t;

enum {
        MAX_IOVEC_LEN = 64,
        MAX_FILENAME_LEN = ((NAME_MAX > 512)
                            ? NAME_MAX
                            : 512)
};

inline
void iovec_init(iovec_t &iov,
                void *data,
                size_t size) {
        iov.iov_base = data;
        iov.iov_len = size;
}

static const file_t BAD_FILE = -1;
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

typedef struct ::stat file_status;

inline
size_t get_size(const file_status &p_status) {
        return p_status.st_size;
}

inline
bool is_directory(const file_status &p_status) {
        return S_ISDIR(p_status.st_mode);
}

inline
bool is_regular(const file_status &p_status) {
        return S_ISREG(p_status.st_mode);
}

struct file_info
{
        std::string m_name; // 文件名称，不包含路径
        int m_type;
};
		
inline
bool is_regular(const file_info &p_info) {
        return S_ISREG(p_info.m_type);
}

inline
bool is_directory(const file_info &p_info) {
        return S_ISDIR(p_info.m_type);
}

inline
const char *get_name(const file_info &p_info) {
        return p_info.m_name.c_str();
}

inline
file_t open(const char *p_path,
            mode_t p_mode = MT_O_RDONLY) {
        return ::open(p_path,
                      static_cast<int>(p_mode));
}

inline
file_t open(const char *p_path,
            mode_t p_mode,
            std::size_t /*replica_number*/) {
        return open(p_path, p_mode);
}

inline
file_t create(const char *p_path) {
        return ::creat(p_path,
                       S_IRWXU | S_IRWXG | S_IRWXO);
}

inline
file_t create(const char *p_path,
              std::size_t /*replica_number*/) {
        return create(p_path);
}

inline
ssize_t read(file_t p_file,
             void *p_buffer,
             size_t p_count) {
        return ::read(p_file, p_buffer, p_count);
}

inline
offset_t append(file_t p_file,
                const void *p_buffer,
                size_t p_count) {
        const offset_t cur = ::lseek(p_file, offset_t(0), SEEK_CUR);
        const ssize_t ret = ::write(p_file, p_buffer, p_count);
        return (ret < 0) ? BAD_OFFSET : cur;
}

inline
ssize_t write(file_t p_file,
              const void *p_buffer,
              size_t p_count) {
        return ::write(p_file, p_buffer, p_count);
}

inline
ssize_t writev(file_t p_file,
               const iovec_t *p_iov,
               size_t p_count) {
        return ::writev(p_file,
                        p_iov,
                        p_count);
}

inline
offset_t seek(file_t p_file,
              offset_t p_offset,
              seek_t p_whence) {
        return ::lseek(p_file,
                       p_offset,
                       static_cast<int>(p_whence));
}

inline
bool close(file_t p_file) {
        return ::close(p_file) == 0;
}

inline
bool mkdir(const char *p_path) {
        return ::mkdir(p_path,
                       S_IRWXU | S_IRWXG | S_IRWXO) == 0;
}

inline
bool rename(const char *p_old_path,
            const char *p_new_path) {
        return std::rename(p_old_path,
                           p_new_path) == 0;
}

inline
bool exists(const char *p_path) {
        return ::access(p_path, F_OK) == 0;
}

inline
bool stat(file_status &p_status,
          const char *p_path) {
        return ::lstat(p_path, &p_status) == 0;
}

// 这里列出的是目录下的文件名称
// FileInfoContainer - fs::file_info container,
//                     and has push_back() method
template<typename FileInfoContainer>
inline
bool list_files(FileInfoContainer &p_infos,
                const char *p_path) {
        dir_t _dir = ::opendir(p_path);
        if (_dir == NULL)
                return false;
        
        file_info _info;
        struct ::dirent *_entry;
        
        std::string _path = p_path;
        if(_path[_path.size() - 1] != '/')
        {
                _path += '/';
        }
        file_status _status;
        while((_entry = readdir(_dir)) != NULL)
        {
                _info.m_name = _entry->d_name;
                
                // 注意，如果有. ..的话，要过滤掉
                if (_info.m_name == "." ||
                    _info.m_name == "..")
                        continue;
                
                if(stat(_status, (_path + _info.m_name).c_str()))
                        _info.m_type = _status.st_mode;
                else
                        _info.m_type = 0; // set to invalid
                
                p_infos.push_back(_info);
        }
        
        ::closedir(_dir);
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
        while(_readed < p_count) {
                ssize_t _ret = read(p_file, _pos, p_count - _readed);
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
                if(_ret >= 0)
                {
                        _writen += _ret;
                        _pos += _ret;
                }
                else
                {
                        if((errno != EAGAIN) &&
                           (errno != EWOULDBLOCK))
                        {
                                return ((_writen == 0)
                                        ? ssize_t(-1)
                                        : ssize_t(_writen)); // -1 or writen len
                        }
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
        return ::pread(p_file, p_buffer, p_count, p_offset);
}

inline
ssize_t pwrite(file_t p_file,
               const void *p_buffer,
               size_t p_count,
               offset_t p_offset) {
        return ::pwrite(p_file, p_buffer, p_count, p_offset);
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

// 如果是目录，则递归删除其子目录和文件；与gfs::remove语义保持一致
inline
bool remove(const char *p_path) {
        if(is_directory(p_path))
        {
                std::vector<file_info> files;
                if(list_files(files, p_path))
                {
                        std::string path_prefix = p_path;
                        path_prefix += '/';
                        for(std::size_t i = 0; i < files.size(); ++i)
                        {
                                if(! remove((path_prefix + get_name(files[i])).c_str()))
                                {
                                        return false;
                                }
                        }
                        return ::rmdir(p_path) == 0;
                }
                else
                {
                        return false;
                }
        }
        else
        {
                return ::unlink(p_path) == 0;
        }
}

} // namespace localfs

#endif	// _LOCALFS_HPP_
