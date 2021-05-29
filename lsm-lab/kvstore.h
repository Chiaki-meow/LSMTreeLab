#pragma once

#include "kvstore_api.h"
#include "MurmurHash3.h"
#include "utils.h"

#include <iostream>
#include <vector>
#include <fstream>
#include <string>
#include <bitset>

#define bfsize 10240*8

using namespace std;

class KVPair{
public:
    uint64_t key;
    std::string value;
};

struct KOPair{
    uint64_t key;
    uint32_t offset;
};

template<typename K, typename V>
struct Node{
    Node<K, V> *right,*down;   //向右向下足矣
    K key;
    V val;
    Node(Node<K, V> *right,Node<K, V> *down, K key, V val): right(right), down(down), key(key), val(val){}
    Node(): right(nullptr), down(nullptr) {}
};

template<typename K, typename V>
class Skiplist {
public:
    Node<K, V> *head;
    Skiplist() {
        head = new Node<K, V>();  //初始化头结点
    }

    size_t size(){
        size_t num=0;
        Node<K,V> *p=head;
        while(p->down)
            p=p->down;
        while(p->right){
            p=p->right;
            num++;
        }
        return num;
    }

    V get(const K& key) {
        Node<K, V> *p = head;
        while(p){
            while(p->right && p->right->key < key){
                p = p->right;
            }
            if(p->right && p->right->key==key){
                p = p->right;
                return (p->val);
            }
            p = p->down;
        }
        return "";
    }

    void put(const K& key, const V& val) {
        vector<Node<K, V>*> pathList;    //从上至下记录搜索路径
        Node<K, V> *p = head;
        while(p){
            while(p->right && p->right->key < key){
                p = p->right;
            }
            pathList.push_back(p);
            p = p->down;
        }

        bool insertUp = true;
        Node<K, V>* downNode= nullptr;
        while(insertUp && pathList.size() > 0){   //从下至上搜索路径回溯，50%概率
            Node<K, V> *insert = pathList.back();
            pathList.pop_back();
            insert->right = new Node<K, V>(insert->right, downNode, key, val); //add新结点
            downNode = insert->right;    //把新结点赋值为downNode
            insertUp = (rand()&1);   //50%概率
        }
        if(insertUp){  //插入新的头结点，加层
            Node<K, V> * oldHead = head;
            head = new Node<K, V>();
            head->right = new Node<K, V>(NULL, downNode, key, val);
            head->down = oldHead;
        }
    }

    bool skipSearch(Node<K, V>* &list, Node<K, V>* &p, const K &k){

        while(true){
            while(p->right && p->right->key < k){
                p = p->right;
            }
            if(p->right&&p->right->key==k)
            {
                p=p->right;
                return true;
            }
            list=list->down;
            if(!list)
                return false;
            p=list;
        }

    }

    bool remove(const K& key) {
        //remove之后记得要把表连起来
        if(size()==0)   return false;
        Node<K, V> *list = head;
        Node<K, V> *p = head;
        Node<K, V> *left, *right;
        if(!skipSearch(list,p,key)) return false;
        do{
            left=list;
            Node<K,V> *lower=p->down;
            while(left->right!=p){
                left=left->right;
            }
            right=p->right;
            delete p;
            left->right=right;
            p=lower;
            list=list->down;
        }while(list);

        while(size()!=0&&head->right== nullptr){
            Node<K, V> *head_d = head;
            head=head->down;
            delete head_d;
        }
        return true;
    }

    void clear(){
        if(size()==0)   return;
        Node<K, V> *p , *q;
        Node<K,V> *line = head;
        while(line) {
            p = line;
            line=line->down;
            while (p) {
                q=p->right;
                delete p;
                p=q;
            }
        }
        head=new Node<K,V>;
        head->key=0;
    }
};

struct Header{
    uint64_t time;
    uint64_t pair_num;
    int64_t max,min;
};

class SSTable{
public:
    int size;
    Header Header;
    uint32_t hash[4];
    bitset<bfsize> bloomfilter;
    vector<KOPair> key_offset;

    KOPair tmpKOPair,tmpKOPair_next;

    void BloomSet(uint64_t key);
    bool BloomFind(uint64_t &key);
    void StoreInDisk(string &dir, vector<string>value, int timeStamp);
    bool searchInSSTable(uint64_t key);
    bool binarySearch(uint64_t key);


};

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    int timeStamp = 1;
    uint32_t MemTable_num = 0;
    uint64_t MemTable_size = 0;

    Skiplist<uint64_t,std::string> MemTable;
    vector<SSTable> sstable;

    string dir_sst;
    vector<string> ret;
    int file_num;
    int tmpSize;

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void check_size();  //check the size of Memtable, if larger then store in SSTable

	bool getInSSTable(uint64_t &key,string &retval);
};
