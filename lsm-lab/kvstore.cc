#include "kvstore.h"

void SSTable::BloomSet(uint64_t key) {
    MurmurHash3_x64_128(&key,sizeof(key),1,hash);
    bloomfilter.set(hash[0]%bfsize);
    bloomfilter.set(hash[1]%bfsize);
    bloomfilter.set(hash[2]%bfsize);
    bloomfilter.set(hash[3]%bfsize);
}

void SSTable::StoreInDisk(string &dir, vector<string>value, int timeStamp) {
    string Direc="/level0/"+to_string(timeStamp);
    Direc=dir+Direc+".sst";
    int checkSize;

    if(utils::_mkdir(dir.c_str())) {
        ofstream outFile(Direc, ios::out | ios::binary);
        outFile.write((char*)&Header, sizeof(Header));
        outFile.write((char*)&bloomfilter, sizeof(bloomfilter));
        checkSize= sizeof(Header)+ sizeof(bloomfilter);
        for (int i = 0; i < key_offset.size(); i++) {
            outFile.write((char*)&key_offset[i].key, sizeof(key_offset[i].key));
            outFile.write((char*)&key_offset[i].offset, sizeof(key_offset[i].offset));
            checkSize+= sizeof(key_offset[i].key)+sizeof(key_offset[i].offset);
        }
        for (int i = 0; i < value.size(); i++) {
            outFile.write(value[i].data(), value[i].length());
            checkSize+= value[i].length();
        }
        outFile.close();
    }
}

bool SSTable::BloomFind(uint64_t &key) {
    //4个函数对于的地址都为1才能说明存在，否则不存在
    MurmurHash3_x64_128(&key,sizeof(key),1,hash);
    bitset<bfsize>bitsettmp;
    bitsettmp.set(hash[0]%bfsize);
    bitsettmp.set(hash[1]%bfsize);
    bitsettmp.set(hash[2]%bfsize);
    bitsettmp.set(hash[3]%bfsize);

    if (bloomfilter.test(hash[0]%bfsize)== 0)
        return 0;
    if (bloomfilter.test(hash[1]%bfsize)== 0)
        return 0;
    if (bloomfilter.test(hash[2]%bfsize)== 0)
        return 0;
    if (bloomfilter.test(hash[3]%bfsize)== 0)
        return 0;
    return 1;
}

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    dir_sst=dir;
    string tmpDir,tmpstr;
    SSTable tmpsst;
    vector<string> filename;
    KOPair tmpKO;
    utils::_mkdir(dir.c_str());
    utils::_mkdir(dir_sst.c_str());
    if(!utils::dirExists(dir))
        return;
    if(!utils::dirExists(dir_sst))
        return;
    file_num=utils::scanDir(dir,ret);

    for(int i=0;i<ret.size();i++){
        tmpDir=dir_sst+"/"+ret[i];
        utils::scanDir(tmpDir,filename);
        for(int i=0;i<filename.size();i++){
            tmpDir=tmpDir+"/"+filename[i];
            ifstream inFile(tmpDir,ios::in|ios::binary); //二进制读方式打开
            if(!inFile) {
                continue;
            }
            inFile.read((char*)&tmpsst.Header, sizeof(tmpsst.Header));
            inFile.read((char*)&tmpsst.bloomfilter, sizeof(tmpsst.bloomfilter));

            for (int i = 0; i < tmpsst.Header.pair_num; i++) {
                inFile.read((char*)&tmpKO.key, sizeof(tmpKO.key));
                inFile.read((char*)&tmpKO.offset, sizeof(tmpKO.offset));
                tmpsst.key_offset.push_back(tmpKO);
            }
            sstable.push_back(tmpsst);
            inFile.close();
        }
    }
}

KVStore::~KVStore()
{
    Node<uint64_t,string> *p=MemTable.head;
    vector<KOPair> key_offset;
    KOPair KO;
    uint32_t offset=10240+32+12*MemTable_num;   //byte
    vector<string> value;
    SSTable tmpsst;
    uint64_t pair_num=0;
    int64_t min,max;
    while(p->down){
        p=p->down;
    }
    if(!p->right)
        return;
    min=p->right->key;
    p=p->right;
    while (p){
        max=p->key;
        KO.key=p->key;
        KO.offset=offset;
        tmpsst.BloomSet(p->key);
        key_offset.push_back(KO);
        offset+=p->val.length();
        value.push_back(p->val);
        pair_num++;
        p=p->right;
    }
    MemTable.clear();

    tmpsst.Header.time=timeStamp;
    tmpsst.Header.pair_num=pair_num;
    tmpsst.Header.max=max;
    tmpsst.Header.min=min;
    tmpsst.key_offset=key_offset;
    tmpsst.StoreInDisk(dir_sst,value,timeStamp);
    sstable.push_back(tmpsst);

    timeStamp++;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    MemTable_size+=(12+s.length());
    tmpSize=s.length()+12;
    check_size();

    string tmp=get(key);
    bool flag = MemTable.remove(key);
    if(flag){
        MemTable_size-=12;
        MemTable_size-=tmp.length();
    }
    MemTable.put(key,s);
    MemTable_num=MemTable.size();
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    string retval=MemTable.get(key);
    if(retval!=""&&retval!="~DELETED~"){
        return retval;
    }
    else if(getInSSTable(key,retval)){
        if(retval=="~DELETED~"||retval=="")
            return "";
        return retval;
    }
    else
        return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    string str=get(key);
    if(str=="")
        return false;

    bool flag = MemTable.remove(key);
    MemTable.put(key,"~DELETED~");
    MemTable_num=MemTable.size();

    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    MemTable.clear();
    MemTable_num=MemTable.size();
    utils::scanDir("./data/level0",ret);
    for(int i=0;i<ret.size();i++){
        char* tmp=(char *)ret[i].c_str();
        utils::rmfile(tmp);
    }
}

void KVStore::check_size() {
    Node<uint64_t,string> *p=MemTable.head;
    vector<KOPair> key_offset;
    KOPair KO;
    uint32_t offset=10240+32+12*MemTable_num;   //byte
    vector<string> value;
    SSTable tmpsst;
    uint64_t pair_num=0;
    int64_t min,max;
    string tmpStr;

    if(MemTable_size>2086880){
        while(p->down){
            p=p->down;
        }
        min=p->right->key;
        p=p->right;
        while (p){
            max=p->key;
            KO.key=p->key;
            KO.offset=offset;
            tmpsst.BloomSet(p->key);
            key_offset.push_back(KO);
            offset+=p->val.length();
            value.push_back(p->val);
            pair_num++;
            p=p->right;
        }
        MemTable.clear();

        tmpsst.Header.time=timeStamp;
        tmpsst.Header.pair_num=pair_num;
        tmpsst.Header.max=max;
        tmpsst.Header.min=min;
        tmpsst.key_offset=key_offset;
        tmpsst.StoreInDisk(dir_sst,value,timeStamp);
        sstable.push_back(tmpsst);

        MemTable_size=tmpSize;

        timeStamp++;
    }
}

bool KVStore::getInSSTable(uint64_t &key,string &retval) {
    file_num=utils::scanDir(dir_sst,ret);
    string direc;
    bool flag=false;
    string tmp;
    int l;

    for(int i=0;i<sstable.size();i++){
        /**现在在level0里查找**/
        direc=dir_sst+"/level0/"+to_string(i+1)+".sst";
        if(sstable[i].searchInSSTable(key)){
            ifstream inFile(direc,ios::in|ios::binary); //二进制读方式打开
            if(!inFile) {
                continue;
            }
            flag=true;
            inFile.seekg(0,ios::end);
            int length=inFile.tellg();
            inFile.seekg(sstable[i].tmpKOPair.offset,ios::beg);
            if(sstable[i].tmpKOPair_next.offset){
                int l=sstable[i].tmpKOPair_next.offset-sstable[i].tmpKOPair.offset;
                char tmpChar;
                for(int i=0;i<l;i++){
                    inFile.read(&tmpChar, 1);
                    tmp+=tmpChar;
                }
            }
            else{
                char tmpChar;
                while(inFile.tellg()!=length){
                    inFile.read(&tmpChar, 1);
                    tmp+=tmpChar;
                }
            }
            retval.append(tmp);
            inFile.close();
        }
    }
    return flag;
}

bool SSTable::binarySearch(uint64_t key) {
        int low = 0, high = key_offset.size() - 1, mid;
        while (low <= high)
        {
            mid = (low + high) / 2;
            if (key_offset[mid].key == key)
            {
                tmpKOPair=key_offset[mid];
                tmpKOPair_next=key_offset[mid+1];
                return true;
            }
            else if (key_offset[mid].key > key)
            {
                high = mid - 1;
            }
            else
                low = mid + 1;
        }
        return false;
}

bool SSTable::searchInSSTable(uint64_t key) {
     if(!BloomFind(key))
         return false;
     else if(binarySearch(key))
         return true;
     return false;
}