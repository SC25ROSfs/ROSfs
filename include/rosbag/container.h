#ifndef ROSBAG_CONTAINER_H
#define ROSBAG_CONTAINER_H

#include "rosbag/bag.h"
#include "rosbag/KeyValueStore.h"

#include <ios>
#include <map>
#include <queue>
#include <chrono>
#include <set>
#include <stdexcept>

#include "rosbag/datatype.h"

namespace rosbag {



class ROSBAG_STORAGE_DECL Container
{
public:

    using TIT = KeyValueStore<rosbag::ROSTimeStamp,rosbag::TIData>;

public:
    Container();
    ~Container();

    explicit Container(std::string const& filename, int root_fd, TIT &&ti, ChunkedFile* header);

    // factory constructor to create a container
    // Use should always use this funtion to create a container
    static std::unique_ptr<Container> create(std::string const& filename);
    static std::unique_ptr<Container> open(std::string const& filename);
    
    void openWrite();

    //! Close the bag file
    void close();

    std::string     getFileName()     const;                      //!< Get the filename of the bag
    uint64_t        getSize()         const;                      //!< Get the current size of the bag file (a lower bound)

    void            setCompression(CompressionType compression);  //!< Set the compression method to use for writing chunks
    CompressionType getCompression() const;                       //!< Get the compression method to use for writing chunks
    void            setChunkThreshold(uint32_t chunk_threshold);  //!< Set the threshold for creating new chunks
    uint32_t        getChunkThreshold() const;                    //!< Get the threshold for creating new chunks


    template<class T>
    void write(std::string const& topic, ros::Time const& time, uint64_t msg_idx, T const& msg, 
               boost::shared_ptr<ros::M_string> connection_header = boost::shared_ptr<ros::M_string>());
    
    void dumpIndex();   // write time_index from buffer to disk

    ROSTimeStamp* getMsgIdx(ROSTimeStamp rts){
        return time_index.get_upper_bound(rts);
    }


private:
    // singeldisable copying
    Container(const Container&) = default;
    Container& operator=(const Container&) = default;
    
    template<class T>
    void doWrite(std::string const& topic, ros::Time const& time, uint64_t msg_idx, T const& msg, boost::shared_ptr<ros::M_string> const& connection_header);

    void closeWrite(ChunkedFile *file);

    void openBrick(ChunkedFile* brick, uint32_t conn_id);

    void startWriting();
    void stopWriting();
    
    // Writing

    void writeVersion();
    void writeFileHeaderRecord(ChunkedFile *file);
    void writeConnectionRecord(ConnectionInfo const* connection_info);
    void appendConnectionRecordToBuffer(Buffer& buf, ConnectionInfo const* connection_info);
    void writeChunkInfoRecords();
    
    template<class T>
    void writeMessageDataRecord(ChunkedFile *file, uint32_t conn_id, ros::Time const& time, T const& msg);
    
    void writeConnectionRecords();

    void readConnectionRecord(ChunkedFile* file,ConnectionInfo* connection_info);
    void readConnectionRecords(ChunkedFile* file,std::size_t conn_count);

    // Reading
    uint32_t getChunkOffset(ChunkedFile *file, uint32_t conn_id);


    // Record header I/O
    void readHeader(ChunkedFile *file,ros::M_string& header);
    void writeHeader(ChunkedFile *file, ros::M_string const& fields);
    void writeDataLength(ChunkedFile *file, uint32_t data_len);
    void appendHeaderToBuffer(Buffer& buf, ros::M_string const& fields);
    void appendDataLengthToBuffer(Buffer& buf, uint32_t data_len);

    // Header fields

    template<typename T>
    std::string toHeaderString(T const* field) const;
    
    std::string toHeaderString(ros::Time const* field) const;

    template <typename T>
    T fromHeaderString(const std::string& str) {
        static_assert(std::is_integral<T>::value, "T must be an integral type");
    
        if (str.size() != sizeof(T)) {
            throw std::invalid_argument("String length is not equal to sizeof(T)");
        }
    
        T value;
        std::memcpy(&value, str.data(), sizeof(T));
        return value;
    }

    ros::Time fromHeaderString(const std::string& str) const {
        if (str.size() != sizeof(uint64_t)) {
            throw std::invalid_argument("String length is not equal to sizeof(uint64_t)");
        }
        uint64_t packed_time;
        std::memcpy(&packed_time, str.data(), sizeof(packed_time));
    
        uint32_t sec = static_cast<uint32_t>(packed_time & 0xFFFFFFFF);
        uint32_t nsec = static_cast<uint32_t>(packed_time >> 32);
    
        return ros::Time(sec, nsec);
    }
    
    // Low-level I/O
    
    void write(ChunkedFile *file, char const* s, std::streamsize n);
    void write(ChunkedFile *file, std::string const& s);
    // void read(char* b, std::streamsize n) const;
    void seek(ChunkedFile *file, uint64_t pos, int origin = std::ios_base::beg) const;
    void read(ChunkedFile* file, char* b, std::streamsize n) const;
    void read(ChunkedFile* file, std::string& s, std::streamsize n) const;
    

private:
    std::string     dir_name_;
    BagMode         mode_;
    int             root_fd_;
    TIT             time_index;
    ChunkedFile*    header_brick;

    CompressionType compression_;                  
    // uint32_t        chunk_threshold_;

    std::map<uint32_t, ChunkedFile*>     brick_files_;
    std::map<uint32_t, uint32_t>         brick_chunk_counts;
    std::map<uint32_t, uint64_t>         brick_filesize_;
    
    uint32_t container_size_; 
    
    uint32_t connection_count_;
    uint64_t file_header_pos_;
    uint64_t index_data_pos_;
    uint32_t chunk_count_;

    ChunkInfo chunk_info_;
    bool chunk_open_;

    uint64_t msg_start_time_;
    bool     is_recording_;

    ros::Time start_time_;
    ros::Time end_time_;

    // Connection indexes
    std::map<std::string, uint32_t>                topic_connection_ids_;
    std::map<ros::M_string, uint32_t>              header_connection_ids_;    
    std::map<uint32_t, ConnectionInfo*>            connections_;
    std::map<uint32_t, uint32_t>                   connection_counts_;   //!< number of messages in each connection 

    std::queue<IndexBuffer>       index_buffer_;
    boost::mutex                  index_mutex_;      //!< mutex for index buffer


    // std::vector<ChunkInfo>                         chunks_;

    mutable Buffer   record_buffer_;           //!< reusable buffer in which to assemble the record data before writing to file

    mutable Buffer   chunk_buffer_;            //!< reusable buffer to read chunk into
    mutable Buffer   decompress_buffer_;       //!< reusable buffer to decompress chunks into

    mutable Buffer   outgoing_chunk_buffer_;   //!< reusable buffer to read chunk into

    mutable Buffer*  current_buffer_;
};

} // namespace rosbag


namespace rosbag {

template<class T>
void Container::write(std::string const& topic, ros::Time const& time, uint64_t msg_idx,
                      T const& msg, boost::shared_ptr<ros::M_string> connection_header) 
{
    doWrite(topic, time, msg_idx, msg, connection_header);
}

template<typename T>
std::string Container::toHeaderString(T const* field) const {
    return std::string((char*) field, sizeof(T));
}

template<class T>
void Container::doWrite(std::string const& topic, ros::Time const& time, uint64_t msg_idx,
                  T const& msg, boost::shared_ptr<ros::M_string> const& connection_header) 
{
    if (time < ros::TIME_MIN)
    {
        throw BagException("Tried to insert a message with time less than ros::TIME_MIN");
    }
    
    // Get ID for connection header
    ConnectionInfo* connection_info = nullptr;
    uint32_t conn_id = 0;
    if (!connection_header) {
        auto topic_conn_ids_iter = topic_connection_ids_.find(topic);
        if (topic_conn_ids_iter == topic_connection_ids_.end()) {
            conn_id = connections_.size();
            topic_connection_ids_[topic] = conn_id;
        } else {
            conn_id = topic_conn_ids_iter->second;
            connection_info = connections_[conn_id];
        }
    } 
    else {
        ros::M_string connection_header_copy(*connection_header);
        connection_header_copy["topic"] = topic;

        auto header_connection_ids_iter = header_connection_ids_.find(connection_header_copy);
        if (header_connection_ids_iter == header_connection_ids_.end()) {
            conn_id = connections_.size();
            header_connection_ids_[connection_header_copy] = conn_id;
        }
        else {
            conn_id = header_connection_ids_iter->second;
            connection_info = connections_[conn_id];
        }        
    }
    // get ptr to Chunkfile from brick_files
    ChunkedFile* brick = nullptr;
    if (brick_files_.find(conn_id) == brick_files_.end()) {
        brick = new ChunkedFile();
        openBrick(brick, conn_id);       
        ROS_INFO("create new brick");
    } else {
        brick = brick_files_[conn_id];
    }

    // Seek to the end of the file (needed in case previous operation was a read)
    seek(brick, 0, std::ios::end);
    brick_filesize_[conn_id] = brick->getOffset();
    if (!chunk_open_) {
        chunk_info_.pos        = 0;
        chunk_info_.start_time = time;
        chunk_info_.end_time   = time;
        chunk_open_ = true;
    }


    // Write connection info record, if necessary
    if (connection_info == nullptr) {
        connection_info = new ConnectionInfo();
        connection_info->id       = conn_id;
        connection_info->topic    = topic;
        connection_info->datatype = std::string(ros::message_traits::datatype(msg));
        connection_info->md5sum   = std::string(ros::message_traits::md5sum(msg));
        connection_info->msg_def  = std::string(ros::message_traits::definition(msg));
        if (connection_header != nullptr) {
            connection_info->header = connection_header;
        }
        else {
            connection_info->header = boost::make_shared<ros::M_string>();
            (*connection_info->header)["type"]               = connection_info->datatype;
            (*connection_info->header)["md5sum"]             = connection_info->md5sum;
            (*connection_info->header)["message_definition"] = connection_info->msg_def;
        }
        connections_[conn_id] = connection_info;
        // No need to encrypt connection records in chunks
        writeConnectionRecord(connection_info);
        appendConnectionRecordToBuffer(outgoing_chunk_buffer_, connection_info);
    }

    // Increment the connection count
    chunk_info_.connection_counts[connection_info->id]++;
    
    uint64_t curr_time_sec = time.sec;
    if (msg_start_time_ == 0 && is_recording_ == false) {
        // ROS_INFO("dowrite:time sec: %u , nsec: %u", time.sec,time.nsec);
        start_time_ = time;
        // ROS_INFO("dowrite:start_time_ sec: %u , nsec: %u", start_time_.sec,start_time_.nsec);
        msg_start_time_ = time.sec;
        is_recording_ = true;
    }
    uint64_t offset = brick->getOffset();

    // Write the message data
    writeMessageDataRecord(brick, conn_id, time, msg);
    stopWriting();
    time_index.insert(rosbag::ROSTimeStamp(time.sec,msg_idx),rosbag::TIData{.conn=conn_id,.offset=offset});
}

template<class T>
void Container::writeMessageDataRecord(ChunkedFile *file, uint32_t conn_id, ros::Time const& time, T const& msg) {
    ros::M_string header;
    header[OP_FIELD_NAME]         = toHeaderString(&OP_MSG_DATA);
    header[CONNECTION_FIELD_NAME] = toHeaderString(&conn_id);
    header[TIME_FIELD_NAME]       = toHeaderString(&time);

    // Assemble message in memory first, because we need to write its length
    uint32_t msg_ser_len = ros::serialization::serializationLength(msg);

    record_buffer_.setSize(msg_ser_len);
    
    ros::serialization::OStream s(record_buffer_.getData(), msg_ser_len);

    // todo: serialize into the outgoing_chunk_buffer & remove record_buffer_
    ros::serialization::serialize(s, msg);

    // We do an extra seek here since writing our data record may
    // have indirectly moved our file-pointer if it was a
    // MessageInstance for our own bag
    seek(file, 0, std::ios::end);
    brick_filesize_[conn_id] = file->getOffset();

    writeHeader(file, header);
    writeDataLength(file, msg_ser_len);
    write(file, (char*) record_buffer_.getData(), msg_ser_len);
    // Update the current chunk time range
    if (time > chunk_info_.end_time)
        chunk_info_.end_time = time;
    if (time < chunk_info_.start_time || (chunk_info_.start_time==ros::Time(0,0))){
        chunk_info_.start_time = time;
    }  
        
}

} // namespace rosbag

#endif
