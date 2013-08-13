// -*-mode:c++; coding:utf-8-*-

#ifndef _XBASE_FILESYSTEM_HPP_
#error "fs.ipp can ONLY be included into fs.hpp"
#endif

// 引入boost的path
typedef boost::filesystem::path path;

struct close_guard
{
	file_t m_file;
			
	close_guard(file_t file)
		: m_file(file) {}
				
	~close_guard() {
		if(m_file != BAD_FILE)
		{
			close(m_file);
		}
	}
};

inline
void init(const std::string &p_conf) {
	p_conf.empty() ? init() : init(p_conf.c_str());
}

inline
file_t open(const std::string &p_path,
	    mode_t p_mode = MT_O_RDONLY) {
	return open(p_path.c_str(), p_mode);
}

inline
file_t open(const std::string &p_path,
	    mode_t p_mode,
	    std::size_t replica_number) {
	return open(p_path.c_str(), p_mode, replica_number);
}

inline
file_t open(const path &p_path,
	    mode_t p_mode = MT_O_RDONLY) {
	return open(p_path.string(), p_mode);
}

inline
file_t open(const path &p_path,
	    mode_t p_mode,
	    std::size_t replica_number) {
	return open(p_path.string(), p_mode, replica_number);
}

inline
file_t create(const std::string &p_path) {
	return create(p_path.c_str());
}

inline
file_t create(const std::string &p_path,
	      std::size_t replica_number) {
	return create(p_path.c_str(), replica_number);
}

inline
file_t create(const path &p_path) {
	return create(p_path.string());
}

inline
file_t create(const path &p_path,
	      std::size_t replica_number) {
	return create(p_path.string(), replica_number);
}

inline
bool remove(const std::string &p_path) {
	return remove(p_path.c_str());
}

inline
bool remove(const path &p_path) {
	return remove(p_path.string());
}

inline
bool rename(const std::string &p_old_path,
	    const std::string &p_new_path) {
	return rename(p_old_path.c_str(),
		      p_new_path.c_str());
}

inline
bool rename(const path &p_old_path,
	    const path &p_new_path) {
	return rename(p_old_path.string(),
		      p_new_path.string());
}

inline
bool exists(const std::string &p_path) {
	return exists(p_path.c_str());
}

inline
bool exists(const path &p_path) {
	return exists(p_path.string());
}

inline
bool stat(file_status &p_status,
	  const std::string &p_path) {
	return stat(p_status, p_path.c_str());
}

inline
bool stat(file_status &p_status,
	  const path &p_path) {
	return stat(p_status, p_path.string());
}

inline
bool mkdir(const std::string &p_path) {
	return mkdir(p_path.c_str());
}

inline
bool mkdir(const path &p_path) {
	return mkdir(p_path.string());
}

template<typename FileInfoContainer>
inline
bool list_files(FileInfoContainer &p_infos,
		const std::string &p_path) {
	return list_files(p_infos, p_path.c_str());
}

template<typename FileInfoContainer>
inline
bool list_files(FileInfoContainer &p_infos,
		const path &p_path) {
	return list_files(p_infos, p_path.string());
}

inline
bool is_regular(const std::string &p_path) {
	return is_regular(p_path.c_str());
}

inline
bool is_regular(const path &p_path) {
	return is_regular(p_path.string());
}

inline
bool is_directory(const std::string &p_path) {
	return is_directory(p_path.c_str());
}

inline
bool is_directory(const path &p_path) {
	return is_directory(p_path.string());
}

inline
offset_t append(file_t p_file,
		const boost::asio::const_buffer &p_buffer) {
	return append(p_file,
		      boost::asio::buffer_cast<const char*>(p_buffer),
		      boost::asio::buffer_size(p_buffer));
}

inline
ssize_t readn(file_t p_file,
	      const boost::asio::mutable_buffer &p_buffer) {
	return readn(p_file,
		     boost::asio::buffer_cast<char*>(p_buffer),
		     boost::asio::buffer_size(p_buffer));
}

inline
ssize_t writen(file_t p_file,
	       const boost::asio::const_buffer &p_buffer) {
	return writen(p_file,
		      boost::asio::buffer_cast<const char*>(p_buffer),
		      boost::asio::buffer_size(p_buffer));
}

template<typename MutableBufferSequence>
inline
ssize_t readn(file_t p_file,
	      const MutableBufferSequence &p_buffer) {
	typename MutableBufferSequence::const_iterator iter = p_buffer.begin();
	typename MutableBufferSequence::const_iterator end = p_buffer.end();
	ssize_t total_bytes_readed = 0;
	for(;iter != end; ++ iter)
	{
		boost::asio::mutable_buffer buffer(*iter);
		ssize_t bytes_readed = readn(p_file, buffer);
		if(bytes_readed < (ssize_t)boost::asio::buffer_size(buffer)) // maybe end or error
		{
			return total_bytes_readed == 0 ? bytes_readed : total_bytes_readed;
		}
		else if(bytes_readed == 0) // end
		{
			return total_bytes_readed;
		}
		else
		{
			total_bytes_readed += bytes_readed;
		}
	}
	return total_bytes_readed;
}

template<typename ConstBufferSequence>
inline
ssize_t writen(file_t p_file,
	       const ConstBufferSequence &p_buffer) {
	iovec_t bufs[MAX_IOVEC_LEN];
	typename ConstBufferSequence::const_iterator iter = p_buffer.begin();
	typename ConstBufferSequence::const_iterator end = p_buffer.end();
	ssize_t all_bytes_wrote = 0;
	while(iter != end)
	{
		size_t i = 0;
		size_t total_buffer_size = 0;
		for(i = 0; (iter != end) && (i < MAX_IOVEC_LEN); ++i, ++iter)
		{
			boost::asio::const_buffer buffer(*iter);
			iovec_init(bufs[i],
				   const_cast<void*>(boost::asio::buffer_cast<const void*>(buffer)),
				   boost::asio::buffer_size(buffer));
			total_buffer_size += boost::asio::buffer_size(buffer);
		}
		if(total_buffer_size > 0)
		{
			ssize_t bytes_wrote = writev(p_file, bufs, i);
			if(bytes_wrote < static_cast<ssize_t>(total_buffer_size))
			{
				return (all_bytes_wrote == 0) ? bytes_wrote : all_bytes_wrote;
			}
			else if(bytes_wrote == 0)
			{
				return all_bytes_wrote;
			}
			else
			{
				all_bytes_wrote += bytes_wrote;
			}
		}
	}
	return all_bytes_wrote;
}
