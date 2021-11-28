#include <iostream>
#include <set>
#include <vector>
#include <unordered_set>
#include <thread>
#include <windows.h>
#include <chrono>
#include <mutex>
#include <string>
#include <algorithm>
#include <exception>
#include <ctime>
#include <string>
#include <map>
#include <list>
#include <sstream>
using namespace std;


class Util
{
    public:
   
    bool isFloat(const string& myString ) 
    {
        std::istringstream iss(myString);
        float f;
        iss >> noskipws >> f; 
        return iss.eof() && !iss.fail(); 
    }

    bool is_number(const string& s)
    {
        std::string::const_iterator it = s.begin();
        while (it != s.end() && std::isdigit(*it)) ++it;
        return !s.empty() && it == s.end();
    }

    bool is_boolean(const string& s)
    {
        if(s == "true" or s == "True" or s=="false" or s=="False") return true;
        return false;
    }

    string checkType(string& s)
    {
        if(isFloat(s)) return string("Double");
        if(is_number(s)) return string("integer");
        if(is_boolean(s)) return string("bool");
        return string("string");
    }
};


class KVStore
{
    private:
     map<string, list<string>> mainMap;
     map<string, string> valMap;
     map<string, string> typeMap;
     map<string, list<string>> secondaryMap;
     Util utilObj;
     mutex m_mutex;

    public:
      
      bool store(string key, string val);
      string get(string key);
      bool update(string key, string val);
      bool deletekey(string key);
      list<string> getSecondaryIndex(string predicateKey, string predicateValue);

};

bool KVStore::deletekey(string key)
{
    lock_guard<mutex> mlock(m_mutex);
    try
    {
        if(mainMap.find(key)==mainMap.end())
         {
            throw string("Key does not Exists");
         }
        for(string st : mainMap[key])
        {
            string n_key = key + "_" + st;
            string val = valMap[n_key];
            valMap.erase(n_key);
            if(secondaryMap[st+ "_" + val].size()>1)
               secondaryMap[st+ "_" + val].remove(key);
            else
                secondaryMap.erase(st+ "_" + val);
        }
        mainMap.erase(key);
    }
    catch(string& e)
    {
        std::cerr<<e<<endl;
        return false;
    }
    return true;
}

string KVStore::get(string key)
{
    lock_guard<mutex> mlock(m_mutex);
    try
    {
       if(mainMap.find(key)==mainMap.end())
       {
         throw string("Key does not Exists");
       }
       string res = "";
       for(string str : mainMap[key])
       {
           string val = valMap[key + "_" + str];
           res = res + str + ":" + val + ",";
       }
        res.pop_back();
        return res;
    }
    catch(string& e)
    {
        cerr<<e<<endl;
    }
    return "";
}
bool KVStore::store(string k, string val)
{
  lock_guard<mutex> mlock(m_mutex);
  try
  {
     //check if Mainkey exists
     if(mainMap.find(k)!=mainMap.end())
     {
         throw string("Key Already Exists");
     }

    val.erase(remove(val.begin(),val.end(),' '),val.end());
  
    string word;
    int start=0;
    int i=1;
    int n = val.length()-1;
 
    while(i<n and val[i] != '}')
    {
      start=i;
      while(i<n and val[i]!=':') 
      {
          i++;
      } 
      string subKey = val.substr(start, i-start);  
      
      start = ++i;  
      while(i<n and val[i]!=',') 
        {
            i++;
        }
    
      string subVal = val.substr(start,i-start);
      ++i;
    
     //check subKey type
     if(typeMap.find(subKey)!=typeMap.end())
     {
         if(typeMap[subKey] != utilObj.checkType(subVal))
          {
              throw string("Type Mismatch!!");
          }
     }
     else
     {
          typeMap[subKey] = utilObj.checkType(subVal);
     }
     mainMap[k].push_front(subKey);
     valMap[k + "_" + subKey] = subVal;
     secondaryMap[subKey+ "_" + subVal].push_back(k);
   }
  }
  catch(string& e)
  {
      std::cerr<<e<<endl;
      return false;
  }
    return true;
}

bool  KVStore::update(string k, string val)
{
   
   lock_guard<mutex> mlock(m_mutex);
   try
    {
     //check if Mainkey exists
     if(mainMap.find(k)==mainMap.end())
     {
         throw string("Key does not Exists");
     }

    val.erase(remove(val.begin(),val.end(),' '),val.end());
  
    string word;
    int start=0;
    int i=1;
    int n = val.length()-1;
    
    while(i<n and val[i] != '}')
    {
      start=i;
      while(i<n and val[i]!=':') 
      {
          i++;
      } 
      string subKey = val.substr(start, i-start);  
      start = ++i;  

      while(i<n and val[i]!=',') 
        {
            i++;
        }
    
      string subVal = val.substr(start,i-start);
      ++i;

     //check subKey type
     if(typeMap.find(subKey)!=typeMap.end())
     {
         if(typeMap[subKey] != utilObj.checkType(subVal))
          {
              throw string("Type Mismatch!!");
          }
     }
     else
     {
          typeMap[subKey] = utilObj.checkType(subVal);
     }

     //update valMap
     string oldVal = "";
     if(valMap.find(k + "_" + subKey)!=valMap.end())
     {
         oldVal = valMap[k + "_" + subKey];
         valMap[k + "_" + subKey] = subVal;
         if(secondaryMap[subKey+ "_" + oldVal].size()>1)
           secondaryMap[subKey+ "_" + oldVal].remove(k);
         else
            secondaryMap.erase(subKey+ "_" + oldVal);
     }
     else
      {
          mainMap[k].push_front(subKey);
          valMap[k + "_" + subKey] = subVal;
      }

       secondaryMap[subKey + "_" + subVal].push_back(k);
   }
  }
  catch(string& e)
  {
      std::cerr<<e<<endl;
      return false;
  }
    return true;
}

list<string> KVStore::getSecondaryIndex(string pkey, string pval)
{
   lock_guard<mutex> mlock(m_mutex);
   try
   {
      if(secondaryMap.find(pkey+ "_" + pval) ==  secondaryMap.end())
        {
            throw string("Not such secondary index exists!!");
        }
          
   }
   catch(const std::exception& e)
   {
       std::cerr << e.what() << '\n';
       return {};
   }
    return secondaryMap[pkey+ "_" + pval];
}


/* 
{delhi: {pollution_level: very high, population:10 M}}
{mumbai: {pollution_level: high}}
*/
int main()
{
    string s = "{pollution_level: very high, population:10 M}";
    string s1 =  "{pollution_level: very high, population:1000000, is_val:0}";
    KVStore obj;
    cout<<obj.store("delhi", s)<<endl;
    cout<<obj.store("delhi","")<<endl;
    cout<<obj.store("molp",s1)<<endl;
    cout<<obj.store("de","{pollution_level : very high, population:10 M, is_val: true  }")<<endl;
    cout<<"get call\n";
    cout<<obj.get("delhi")<<endl;
    cout<<obj.update("delhi", "{country:india, manufacturer: glx}")<<endl;
    cout<<obj.get("delhi")<<endl;

    cout<<"GetSecondary\n";
    for(string st : obj.getSecondaryIndex("population","10M")) cout<<st<<endl;
    cout<<"end GetSecondary\n";
    
    cout<<obj.deletekey("de")<<endl;
    cout<<obj.get("de")<<endl;
    
    return 0;
}