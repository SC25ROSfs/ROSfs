#ifndef DATA_TYPE_H
#define DATA_TYPE_H

#include <cstdint>
#include <iostream>

namespace rosbag {

    struct TIData {
        uint32_t conn;
        uint64_t offset;
    };

    struct ROSTimeStamp {
        uint64_t sec;
        uint64_t idx;

        ROSTimeStamp(uint64_t seconds, uint64_t idx)
            : sec(seconds), idx(idx) {}

        bool operator<(const ROSTimeStamp& rhs) const {
            if (sec != rhs.sec) {
                return sec < rhs.sec;
            }
            return idx < rhs.idx;
        }

        bool operator>(const ROSTimeStamp& rhs) const {
            if (sec!=rhs.sec) {
                return sec > rhs.sec;
            }

            return idx > rhs.idx;
        }

        bool operator==(const ROSTimeStamp& rhs) const {
            return sec == rhs.sec && idx == rhs.idx;
        }

        bool operator>=(const ROSTimeStamp& rhs) const {
            if (sec != rhs.sec) {
                return sec > rhs.sec;
            }

            return idx >= rhs.idx;
        }

        bool operator<=(const ROSTimeStamp& rhs) const {
            if (sec != rhs.sec) {
                return sec < rhs.sec;
            }

            return idx <= rhs.idx;
        }

        friend std::ostream& operator<<(std::ostream& os, const ROSTimeStamp& time) {
            os << "ROSTimeStamp(sec: " << time.sec << ", idx: " << time.idx << ")";
            return os;
        }
    };

    struct IndexBuffer {
        uint32_t conn;      // connection id
        ROSTimeStamp time;      // timestamp
        uint64_t offset;    // offset in brick file
        IndexBuffer(uint32_t con, ROSTimeStamp t,  uint64_t off):
        conn{con}, time{t}, offset{off} {} 
    };

    struct multiKey {
        uint64_t key1;
        uint64_t key2;

        bool operator==(const multiKey& other) const {
            return key1 == other.key1 && key2 == other.key2;
        }

        bool operator<(const multiKey& other) const {
            if (key1 != other.key1) {
                return key1 < other.key1;
            }
            return key2 < other.key2;
        }

        bool operator>(const multiKey& other) const {
            if (key1 != other.key1) {
                return key1 > other.key1;
            }
            return key2 > other.key2;
        }

        bool operator<=(const multiKey& other) const {
            if (key1 != other.key1) {
                return key1<other.key1;
            }

            if (key2 > other.key2) {
                return false;
            }

            return true;
        }

        bool operator>=(const multiKey& other) const {
            if (key1 != other.key1) {
                return key1>other.key1;
            }

            if (key2<other.key2){
                return false;
            }

            return true;
        }

        friend std::ostream& operator<<(std::ostream& os, const multiKey& mk) {
            os << mk.key1 << ":" << mk.key2;
            return os;
        }
    };

    struct multiValue {
        uint32_t value1;
        uint64_t value2;

        friend std::ostream& operator<<(std::ostream& os, const multiValue& mv) {
            os << mv.value1 << ":" << mv.value2;
            return os;
        }
    };

    struct Entry {
        multiKey k;
        multiValue v;
    };

    struct kvPair {
        rosbag::ROSTimeStamp ts;
        rosbag::TIData data;
    };
}

#endif