#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <algorithm>
#include <fcntl.h>
#include <vector>

#include "rosbag/KeyValueStore.h"
#include "rosbag/container.h"
#include "rosbag/datatype.h"

#include <string>
#include <boost/shared_ptr.hpp>
#include "ros/time.h"
#include "rosbag/exceptions.h"
#include "topic_tools/shape_shifter.h"
#include "ros/serialization.h"

static PyObject *rosfs_timekv_query(PyObject *self, PyObject *args) {
    const char *path;
    PyObject *tags_obj;
    uint64_t start_sec,end_sec;
    (void)self;

    if (!PyArg_ParseTuple(args, "sOKK", &path, &tags_obj, &start_sec,
                          &end_sec)) {
        return NULL;
    }

    if (!PyList_Check(tags_obj)) {
        PyErr_SetString(PyExc_TypeError, "Expected a list of tags");
        return NULL;
    }

    int dir = open(path, O_PATH);

    if (end_sec < start_sec) {
        PyErr_SetString(PyExc_ValueError,
                        "End time should greater than start time");
        return NULL;
    }

    auto time_index =
        rosbag::KeyValueStore<rosbag::ROSTimeStamp,rosbag::TIData>::open(dir, "time_index", 0x1000);

    if (time_index.get_fd() == -1) {
        close(dir);
        return NULL;
    }

    PyObject *result = PyDict_New();
    Py_ssize_t len = PyList_Size(tags_obj);

    rosbag::ROSTimeStamp start_ts{start_sec,0},end_ts{end_sec,0};
    auto start_iter_all = time_index.lower_bound(start_ts);
    // auto end_iter_all = time_index.lower_bound(end_time);
    
    for (Py_ssize_t i = 0; i < len; i++) {
        PyObject *tag_obj = PyList_GetItem(tags_obj, i);
        if (!PyLong_Check(tag_obj)) {
            PyErr_SetString(PyExc_TypeError, "Expected a list of integers");
            return NULL;
        }

        uint32_t tag = PyLong_AsUnsignedLong(tag_obj);

        uint64_t start_offset = ULLONG_MAX;
        uint64_t last_offset = 0;

        auto start_iter = start_iter_all;
        // auto end_iter = end_iter_all;

        for (auto cur = start_iter.next(); cur != nullptr; cur = start_iter.next()) {
            auto kvp = static_cast<const rosbag::kvPair *>(cur);
            if (kvp->ts > end_ts)
                break;
            if (kvp->data.conn == tag) {
                if (kvp->data.offset < start_offset) {
                    start_offset = kvp->data.offset;
                    // break;
                }
                last_offset = kvp->data.offset;
            }
        }

        // last_offset = end_iter.prev()->value.offset;

        // for (auto cur = end_iter.prev(); cur != nullptr; cur = end_iter.prev()) {
        //     if (cur->value.conn == tag) {
        //         last_offset = cur->value.offset;
        //         break;
        //     }
        // }

        PyObject *offsets = Py_BuildValue("KK", start_offset, last_offset);
        PyDict_SetItem(result, PyLong_FromUnsignedLong(tag), offsets);
        Py_DECREF(offsets);
    }

    return result;

    // return nullptr;
}

static PyObject *rosfs_container_create(PyObject *self,PyObject *args){
    const char* path;

    if (!PyArg_ParseTuple(args,"s",&path)){
        PyErr_SetString(PyExc_TypeError, 
            "connection_header keys/values must be strings");
        return NULL;
    }

    auto container_ptr = rosbag::Container::create(path);
    container_ptr->openWrite();
    container_ptr->close();

    Py_RETURN_NONE;
}

static PyObject *rosfs_timekv_insert(PyObject *self, PyObject *args) {
    const char* path;
    const char* topic;
    uint32_t sec, nsec;
    const char* msg_type;
    const char* md5sum;
    const char* msg_def;
    PyObject* msg_or_bytes_obj;  
    PyObject* conn_header_obj;
    int raw_flag;                

    if (!PyArg_ParseTuple(args, "ssIIsssOOi",
        &path, &topic,
        &sec, &nsec,
        &msg_type, &md5sum, &msg_def,
        &msg_or_bytes_obj,
        &conn_header_obj,
        &raw_flag)) {
            return NULL;
    }

    try{
        ros::Time time(sec, nsec);
        std::vector<uint8_t> buffer;

        if (raw_flag) {
            char* data;
            Py_ssize_t data_size;
            if (PyBytes_AsStringAndSize(msg_or_bytes_obj, &data, &data_size) == -1) {
                return NULL;
            }
            buffer.assign(data, data + data_size);
        } else {
            PyObject* io_module = PyImport_ImportModule("io");
            if (!io_module) {
                PyErr_SetString(PyExc_ImportError, "Failed to import io module");
                return NULL;
            }

            PyObject* bytes_io_class = PyObject_GetAttrString(io_module, "BytesIO");
            Py_DECREF(io_module);
            if (!bytes_io_class) {
                PyErr_SetString(PyExc_AttributeError, "BytesIO class not found");
                return NULL;
            }

            PyObject* buffer_obj = PyObject_CallObject(bytes_io_class, NULL);
            Py_DECREF(bytes_io_class);
            if (!buffer_obj) {
                PyErr_SetString(PyExc_RuntimeError, "Failed to create BytesIO object");
                return NULL;
            }

            PyObject* serialize_method = PyObject_GetAttrString(msg_or_bytes_obj, "serialize");
            if (!serialize_method) {
                Py_DECREF(buffer_obj);
                PyErr_SetString(PyExc_AttributeError, "Message has no serialize method");
                return NULL;
            }

            PyObject* args = PyTuple_Pack(1, buffer_obj);
            PyObject* result = PyObject_CallObject(serialize_method, args);
            Py_DECREF(args);
            Py_DECREF(serialize_method);
            if (!result) {
                Py_DECREF(buffer_obj);
                PyErr_SetString(PyExc_RuntimeError, "Failed to serialize message");
                return NULL;
            }
            Py_DECREF(result);

            PyObject* getvalue_method = PyObject_GetAttrString(buffer_obj, "getvalue");
            if (!getvalue_method) {
                Py_DECREF(buffer_obj);
                PyErr_SetString(PyExc_AttributeError, "BytesIO has no getvalue method");
                return NULL;
            }

            PyObject* serialized_bytes = PyObject_CallObject(getvalue_method, NULL);
            Py_DECREF(getvalue_method);
            Py_DECREF(buffer_obj);
            if (!serialized_bytes) {
                PyErr_SetString(PyExc_RuntimeError, "Failed to get serialized bytes");
                return NULL;
            }

            char* data;
            Py_ssize_t data_size;
            if (PyBytes_AsStringAndSize(serialized_bytes, &data, &data_size) == -1) {
                Py_DECREF(serialized_bytes);
                return NULL;
            }
            buffer.assign(data, data + data_size);
            Py_DECREF(serialized_bytes);
        }

        boost::shared_ptr<ros::M_string> connection_header(new ros::M_string());
        if (conn_header_obj != Py_None) {
            if (!PyDict_Check(conn_header_obj)) {
                PyErr_SetString(PyExc_TypeError, "connection_header must be a dictionary");
                return NULL;
            }

            PyObject *key, *value;
            Py_ssize_t pos = 0;

            while (PyDict_Next(conn_header_obj, &pos, &key, &value)) {
                const char* key_str = PyUnicode_AsUTF8(key);
                const char* value_str = PyUnicode_AsUTF8(value);
                if (!key_str || !value_str) {
                    PyErr_SetString(PyExc_TypeError, 
                        "connection_header keys/values must be strings");
                    return NULL;
                }
                (*connection_header)[key_str] = value_str;
            }
        } else {
            (*connection_header)["topic"] = topic;
            (*connection_header)["type"] = msg_type;
            (*connection_header)["md5sum"] = md5sum;
            (*connection_header)["message_definition"] = msg_def;
        }

        topic_tools::ShapeShifter msg;
        msg.morph(md5sum, msg_type, msg_def, "");
        ros::serialization::IStream stream(buffer.data(), buffer.size());
        msg.read(stream);

        auto container_ptr = rosbag::Container::open(path);
        // generate unique msg id
        // In case the same timestamp appears
        auto rts = container_ptr->getMsgIdx(rosbag::ROSTimeStamp{sec+1,0});
        uint64_t msg_idx;
        if (rts==nullptr){
            msg_idx = 0;
        }else{
            msg_idx = rts->idx + 1;
        }

        container_ptr->write(topic,time,msg_idx,msg,connection_header);
        container_ptr->close();
    } catch (const rosbag::BagException& e) {
        PyErr_SetString(PyExc_IOError, e.what());
        return NULL;
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *rosfs_timekv_insert_batch(PyObject *self, PyObject *args) {
    const char* path;
    PyObject* topics;
    PyObject* secs;
    PyObject* nsecs;
    PyObject* msg_types;
    PyObject* md5sums;
    PyObject* msg_defs;
    PyObject* msg_or_bytes_objs;
    PyObject* conn_header_objs;
    int raw_flag;

    if (!PyArg_ParseTuple(args, "sOOOOOOOOi",
        &path, &topics, &secs, &nsecs,
        &msg_types, &md5sums, &msg_defs,
        &msg_or_bytes_objs, &conn_header_objs,
        &raw_flag)) {
            return NULL;
    }

    // Validate all inputs are lists
    if (!PyList_Check(topics) || !PyList_Check(secs) || !PyList_Check(nsecs) ||
        !PyList_Check(msg_types) || !PyList_Check(md5sums) || !PyList_Check(msg_defs) ||
        !PyList_Check(msg_or_bytes_objs) || !PyList_Check(conn_header_objs)) {
            PyErr_SetString(PyExc_TypeError, "All batch parameters must be lists");
            return NULL;
    }

    Py_ssize_t num_msgs = PyList_Size(topics);
    if (num_msgs != PyList_Size(secs) || num_msgs != PyList_Size(nsecs) ||
        num_msgs != PyList_Size(msg_types) || num_msgs != PyList_Size(md5sums) ||
        num_msgs != PyList_Size(msg_defs) || num_msgs != PyList_Size(msg_or_bytes_objs) ||
        num_msgs != PyList_Size(conn_header_objs)) {
            PyErr_SetString(PyExc_ValueError, "All parameter lists must have the same length");
            return NULL;
    }

    // Preload reusable Python objects
    PyObject* io_module = PyImport_ImportModule("io");
    if (!io_module) {
        PyErr_SetString(PyExc_ImportError, "Failed to import io module");
        return NULL;
    }
    PyObject* bytes_io_class = PyObject_GetAttrString(io_module, "BytesIO");
    Py_DECREF(io_module);
    if (!bytes_io_class) {
        PyErr_SetString(PyExc_AttributeError, "BytesIO class not found");
        return NULL;
    }

    try {
        auto container_ptr = rosbag::Container::open(path);

        for (Py_ssize_t i = 0; i < num_msgs; ++i) {
            // Extract basic parameters
            const char* topic = PyUnicode_AsUTF8(PyList_GetItem(topics, i));
            uint32_t sec = PyLong_AsUnsignedLong(PyList_GetItem(secs, i));
            uint32_t nsec = PyLong_AsUnsignedLong(PyList_GetItem(nsecs, i));
            const char* msg_type = PyUnicode_AsUTF8(PyList_GetItem(msg_types, i));
            const char* md5sum = PyUnicode_AsUTF8(PyList_GetItem(md5sums, i));
            const char* msg_def = PyUnicode_AsUTF8(PyList_GetItem(msg_defs, i));
            PyObject* msg_or_bytes_obj = PyList_GetItem(msg_or_bytes_objs, i);
            PyObject* conn_header_obj = PyList_GetItem(conn_header_objs, i);

            // Serialization logic
            std::vector<uint8_t> buffer;
            if (raw_flag) {
                char* data;
                Py_ssize_t data_size;
                if (PyBytes_AsStringAndSize(msg_or_bytes_obj, &data, &data_size) == -1) {
                    throw std::runtime_error("Invalid raw bytes object");
                }
                buffer.assign(data, data + data_size);
            } else {
                PyObject* buffer_obj = PyObject_CallObject(bytes_io_class, NULL);
                if (!buffer_obj) throw std::runtime_error("Failed to create BytesIO");

                PyObject* serialize_method = PyObject_GetAttrString(msg_or_bytes_obj, "serialize");
                if (!serialize_method) {
                    Py_DECREF(buffer_obj);
                    throw std::runtime_error("Message missing serialize method");
                }
                
                PyObject* args = PyTuple_Pack(1, buffer_obj);
                PyObject* result = PyObject_CallObject(serialize_method, args);
                Py_DECREF(args);
                Py_DECREF(serialize_method);
                if (!result) {
                    Py_DECREF(buffer_obj);
                    PyErr_SetString(PyExc_RuntimeError, "Failed to serialize message");
                    return NULL;
                }
                Py_DECREF(result);
               
                PyObject* getvalue_method = PyObject_GetAttrString(buffer_obj, "getvalue");
                if (!getvalue_method) {
                    Py_DECREF(buffer_obj);
                    PyErr_SetString(PyExc_AttributeError, "BytesIO has no getvalue method");
                    return NULL;
                }
                
                PyObject* serialized_bytes = PyObject_CallObject(getvalue_method, NULL);
                Py_DECREF(getvalue_method);
                Py_DECREF(buffer_obj);
                if (!serialized_bytes) {
                    PyErr_SetString(PyExc_RuntimeError, "Failed to get serialized bytes");
                    return NULL;
                }

                char* data;
                Py_ssize_t data_size;
                if (PyBytes_AsStringAndSize(serialized_bytes, &data, &data_size) == -1) {
                    Py_DECREF(serialized_bytes);
                    return NULL;
                }
                buffer.assign(data, data + data_size);
                Py_DECREF(serialized_bytes);
            }

            // Connection header processing
            boost::shared_ptr<ros::M_string> connection_header(new ros::M_string());
            if (conn_header_obj != Py_None) {
                if (!PyDict_Check(conn_header_obj)) {
                    throw std::runtime_error("connection_header must be a dictionary");
                }

                PyObject *key, *value;
                Py_ssize_t pos = 0;
                while (PyDict_Next(conn_header_obj, &pos, &key, &value)) {
                    const char* k = PyUnicode_AsUTF8(key);
                    const char* v = PyUnicode_AsUTF8(value);
                    if (!k || !v) throw std::runtime_error("Invalid header key/value");
                    (*connection_header)[k] = v;
                }
            } else {
                (*connection_header)["topic"] = topic;
                (*connection_header)["type"] = msg_type;
                (*connection_header)["md5sum"] = md5sum;
                (*connection_header)["message_definition"] = msg_def;
            }

            // Message morphing and writing
            topic_tools::ShapeShifter msg;
            msg.morph(md5sum, msg_type, msg_def, "");
            ros::serialization::IStream stream(buffer.data(), buffer.size());
            msg.read(stream);

            // Generate msg_idx
            auto rts = container_ptr->getMsgIdx(rosbag::ROSTimeStamp{sec+1,0});
            uint64_t msg_idx = (rts == nullptr) ? 0 : rts->idx + 1;

            container_ptr->write(topic, ros::Time(sec, nsec), msg_idx, msg, connection_header);
        }

        container_ptr->close();
        Py_DECREF(bytes_io_class);
    } catch (const rosbag::BagException& e) {
        Py_DECREF(bytes_io_class);
        PyErr_SetString(PyExc_IOError, e.what());
        return NULL;
    } catch (const std::exception& e) {
        Py_DECREF(bytes_io_class);
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyMethodDef RosfsTimekvMethods[] = {
    {"query", rosfs_timekv_query, METH_VARARGS,
     "Query the time key and value."},
    {
        "insert", rosfs_timekv_insert, METH_VARARGS, "Insert the time key and value"
    },
    {
        "create", rosfs_container_create,METH_VARARGS, "create an empty container"
    },
    {
        "batch_insert", rosfs_timekv_insert_batch, METH_VARARGS, "batch insert the time key and value"
    },
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef rosfs_timekvmodule = {
    PyModuleDef_HEAD_INIT,
    "rosfs_timekv", /* name of module */
    NULL,           /* module documentation, may be NULL */
    -1,             /* size of per-interpreter state of the module */
    RosfsTimekvMethods,
    NULL, /* m_slots */
    NULL, /* m_traverse */
    NULL, /* m_clear */
    NULL, /* m_free */
};

PyMODINIT_FUNC PyInit_rosfs_timekv(void) {
    return PyModule_Create(&rosfs_timekvmodule);
}
