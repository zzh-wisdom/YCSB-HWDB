//
// Created by wujy on 1/23/19.
//
#include <iostream>


#include "hwdb_db.h"
#include "lib/coding.h"

using namespace std;

namespace ycsbc {

    struct Data_S{   //适合hwdb的字符串
        char *data_;  //data前四个字节是size_,然后才是数据

        Data_S():data_(nullptr) {};

        Data_S(const char *str){
            uint32_t size_ = strlen(str);
            data_ = new char[sizeof(uint32_t) + size_];
            uint32_t *len = (uint32_t *)data_;
            *len = size_;
            memcpy(data_ + sizeof(uint32_t), str, size_);
        }

        Data_S(const std::string &str){
            uint32_t size_ = str.size();
            data_ = new char[sizeof(uint32_t) + size_];
            uint32_t *len = (uint32_t *)data_;
            *len = size_;
            memcpy(data_ + sizeof(uint32_t), str.c_str(), size_);
        }

        ~Data_S(){
            // 对于自定义类型的数组，当使用delete时，仅仅调用了对象数组中第一个对象的析构函数，而使用delete [ ]的话，将会逐个调用析构函数。
            // 对简单的基本数据类型，delete a与delete []a一样的效果
            if(data_ != nullptr)  delete []data_;
        }

        char *raw_data(){
            return data_;
        }

        uint32_t size(){
            return *(uint32_t *)data_;
        }

        // 注意：返回的char* 没有结尾\0
        char *data(){
            if(data_ != nullptr) return data_ + sizeof(uint32_t);
            return nullptr;
        }

    };

    // dbfilename：数据库数据放置的路径
    HWDB::HWDB(const char *dbfilename, utils::Properties &props) :noResult(0){
    
        //set option
        SetOptions(dbfilename, props);
        

        int s = CreatHwdb((char *)dbfilename, &config_, &db_);
        if(s != 0){
            cerr<<"Can't open hwdb "<<dbfilename<<" "<<s<<endl;
            exit(0);
        }

        // 打印hwdb的内部参数
        PrintConfig(&config_);
    }

    // 设置HWDB数据库的内部参数
    // 这里会覆盖默认的参数
    void HWDB::SetOptions(const char *dbfilename, utils::Properties &props) {
        //
        SetDefaultHwdbConfig(&config_);
        strcpy(config_.fs_path, dbfilename);
        config_.kLogMaxWriteSize = 32 * 1024 * 1024;  // 每个日志大小是：32MB
        config_.kMaxPthreadNum = 6;                   // 等于cpu核数      
        config_.kPthreadDoFlushJobNum = 3;
        
        uint64_t nums = stoi(props.GetProperty(CoreWorkload::RECORD_COUNT_PROPERTY));
        uint32_t key_len = stoi(props.GetProperty(CoreWorkload::KEY_LENGTH));
        uint32_t value_len = stoi(props.GetProperty(CoreWorkload::FIELD_LENGTH_PROPERTY));

        uint32_t inner_size = 512;       // 中间节点块大小
        uint32_t leaf_size = 4096;       // 叶子节点块大小

        config_.kInnerBlockSize = inner_size;
        config_.kInnerCacheItemSize = inner_size;
        config_.kLeafBlockSize = leaf_size;
        config_.kLeafCacheItemSize = leaf_size;

        uint64_t inner_cache = nums * (key_len + value_len) * 5 / 100 / inner_size;
        uint64_t leaf_cache = nums * (key_len + value_len) * 5 / 100 / leaf_size;
        if(inner_cache < 4096) inner_cache = 4 * 4096;
        if(leaf_cache < 1024) leaf_cache = 16 * 1024;

        config_.kInnerCacheBucketSize = inner_cache;
        config_.kInnerCacheBits = 8; //

        config_.kLeafCacheBucketSize = leaf_cache;
        config_.kLeafCacheBits = 8;  //

        config_.kPlogClientType = 3;
        config_.kLogDirectWrite = 0;
    }

    // 简单的查询操作
    int HWDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        Data_S find_key(key);
        char *value = nullptr;
        int s = db_->interface.Getkv(db_->db, find_key.raw_data(), &value);
        //printf("read:key:%lu-%s\n",key.size(),key.c_str());
        if(s == 0) {
            if(value == nullptr){
                noResult++;
                //cerr<<"read not found:"<<noResult<<endl;
                //printf("read not find:key:%lu-%s\n",key.size(),key.c_str());
                return DB::kOK;
            }
            //printf("value:%lu\n",value.size());
            //DeSerializeValues(std::string(value + 4), result);
            /* printf("get:key:%lu-%s\n",key.size(),key.data());
            for( auto kv : result) {
                printf("get field:key:%lu-%s value:%lu-%s\n",kv.first.size(),kv.first.data(),kv.second.size(),kv.second.data());
            } */
            return DB::kOK;
        }
        else{
            cerr<<"read error"<<endl;
            exit(0);
        }
    }


    int HWDB::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        void *iter = nullptr;
        int s = db_->interface.RangekvGet(db_->db, Data_S(key).raw_data(), Data_S(max_key).raw_data(), &iter);
        //printf("scan:key:%lu-%s max_key:%lu-%s\n",key.size(), key.c_str(), max_key.size(), max_key.c_str());
        if(s != 0){
            cerr<<"scan error"<<endl;
            exit(0);
        }
        char *val;
        char *k;
        bool ok = true;
        int i;
        for(i=0;i < len && ok; i++){
            k = NULL;
            val = NULL;
            s = db_->interface.GetNext(iter, &k, &val);
            if(k != NULL && val != NULL){
                free(k);
                free(val);
            }
            else{
                ok = false;
            }
            
        } 
        //printf("scan find:i:%d len:%d\n", i , len);
        db_->interface.DeleteIterator(&iter);
        return DB::kOK;
    }

    // put操作
    int HWDB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        int s;
        string value = values.at(0).second;
        //SerializeValues(values,value);
        /* printf("put:key:%lu-%s\n",key.size(),key.data());
        for( auto kv : values) {
            printf("put field:key:%lu-%s value:%lu-%s\n",kv.first.size(),kv.first.data(),kv.second.size(),kv.second.data());
        } */
        s = db_->interface.Putkv(db_->db, Data_S(key).raw_data(), Data_S(value).raw_data());
        //printf("put:key:%lu-%s\n",key.size(),key.c_str());
        if(s != 0){
            cerr<<"insert error\n"<<endl;
            exit(0);
        }
       
        return DB::kOK;
    }

    // 和Insert一样，put操作
    int HWDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int HWDB::Delete(const std::string &table, const std::string &key) {
        int s;
        s = db_->interface.Delkv(db_->db, Data_S(key).raw_data());
        //printf("delete:key:%lu-%s\n",key.size(),key.c_str());
        if(s != 0){
            cerr<<"Delete error\n"<<endl;
            exit(0);
        }
        return DB::kOK;
    }

    void HWDB::PrintStats() {
        if(noResult) cout<<"read not found:"<<noResult<<endl;
        char stats[4096];
        memset(stats, 0, 4096);
        db_->interface.PrintStats(db_->db, (char *)&stats);
        cout<<stats<<endl;
    }


    HWDB::~HWDB() {
        DeleteHwdb(&db_);
    }

    // 将kvs中的所有key-value序列化成一个字符串。
    void HWDB::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    // 解码序列化
    void HWDB::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
        uint64_t offset = 0;
        uint64_t kv_num = 0;
        uint64_t key_size = 0;
        uint64_t value_size = 0;

        kv_num = DecodeFixed64(value.c_str());
        offset += 8;
        for( unsigned int i = 0; i < kv_num; i++){
            ycsbc::DB::KVPair pair;
            key_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.first.assign(value.c_str() + offset, key_size);
            offset += key_size;

            value_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.second.assign(value.c_str() + offset, value_size);
            offset += value_size;
            kvs.push_back(pair);
        }
    }
}
