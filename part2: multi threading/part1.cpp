/*
Name: Suman Bagale
Course number:
lab number:

 CSE 3320   Operating Systems
 Summer 2021, © DL, UTA, 2021

		 Programming Assignment 2
		 Creating Processes and Threads and Communication between Processes

GOAL: Create a process and threads, run in parallel, and pass information between threads. 
*/


#include <string>
#include <sstream>
#include <iostream>
#include <fstream>   //create files, write information to files, and read information from files.
#include<vector>     
#include<array>
#include<iterator>      
#include<thread>        


using namespace std;

int max_data=1000;

//function declarations
vector<string> split_string(string ,char );  
vector<string> read_file(string);                  
vector<float> get_latitude(vector<string>);
void bubble_sort(vector<float> &);
vector<vector<float>> chunkify_data(vector<float>,int);
vector<vector<float>> threaded_sort(vector<vector<float>>);
vector<float> merge_sorted_chunks(vector<vector<float>>);

int main()
{
   int n_processes;
   const string filename="all_month.csv";
   vector<string> dataset=read_file(filename);    //read csv file
   vector<float> latitude=get_latitude(dataset);  //extract latitude from a dataset
   int n_threads;
   char choice;

   while(1){
      int n_lines;
      cout<<"Enter number of lines:";
      cin>>max_data;
      if(max_data>12448){
         cout<<"Enter value less than 12448";
         cout<<"Enter number of lines:";
      }
      cout<<"Enter number of threads:";
      cin>>n_threads;

      auto start_time = chrono::high_resolution_clock::now();         //start a clock
      
      vector<vector<float>> chunks=chunkify_data(latitude,n_threads);  //segment data into chunks for parallel processing
      vector<vector<float>> sorted_chunks=threaded_sort(chunks);       //use  threads to sort multiple chunks simultaneously
      vector<float> sorted_latitude=merge_sorted_chunks(sorted_chunks); //finish sorting process by merging sorted chunks

      auto end_time = chrono::high_resolution_clock::now();              //stop a clock

      auto sort_time = chrono::duration_cast<chrono::milliseconds>(end_time-start_time).count();   //record a duration of sort 
      
      cout<<"Time taken:"<<sort_time<<" millisecond\n";

      cout<<"Do you want to experiment again (Y/N):";
      cin>>choice;
      if(choice=='N') break;
      cout<<"\n\n";
   }
   
   return 0;
}

vector<float> get_latitude(vector<string> dataset){
   vector<float> latitude;
   char delimiter=',';
   int latitude_column=1;     //csv file latitude column is indexed 1
   for (int row=0; row<max_data ;row++){
      
      
      latitude.push_back(atof(split_string(dataset[row],delimiter)[latitude_column].c_str()));   //atof only accepts c string as argument
   }
   return latitude;
}


vector<string> read_file(string filename)
{
   fstream newfile;
   vector<string> dataset;
   int row=0;
   newfile.open(filename,ios::in);  //open a file to perform read operation using file object
   if (newfile.is_open()){          //checking whether the file is open
      string line;
      getline(newfile, line);        // first line is header so ignoring first line
      while(getline(newfile, line)){ //read data from file object and put it into string called line.
         dataset.push_back(line);      //push line to vector of string
         row++;
         if(row>max_data) break;        //only push 1st max_data lines to vector

      }
      newfile.close(); //close the file object.
   }
   return dataset;
}

vector<string> split_string(string comma_seperated_string,char delimiter){
   stringstream string_stream(comma_seperated_string);  // creating string stream object
	int i=0;            
    vector<string> values;
    
      while(string_stream.good())   // loop will continue if string stream is error free
      {
         string a;       
         getline( string_stream, a, ',' );   //read from string_stream and store it in a

         values.push_back(a);                 //push a to values
         
         i++;
      }
      return values;
}

vector<vector<float>> chunkify_data(vector<float> latitude,int n_processes){
   vector<vector<float>> chunks;
   int n_lines=max_data/n_processes;          //number of line per chunks
   int starting_point;
   int ending_point;
   for(int i=0;i<n_processes;i++){
      vector<float> chunk;
      starting_point=i*n_lines;
      ending_point=i*n_lines+n_lines;

      
      if(i==n_processes-1){
         ending_point=max_data;             //data may not be exactly divisible by number of processes
      }

      for(int j=starting_point;j<ending_point;j++){
         chunk.push_back(latitude[j]);
      }
      chunks.push_back(chunk);   
   } 
   return chunks;
}
/*A thread of execution is a sequence of instructions that can be executed concurrently with other such sequences in 
multithreading environments, while sharing a same address space*/
vector<vector<float>> threaded_sort(vector<vector<float>> chunks){
   
   
   
   vector<thread> threads;                          //initialize vector of threads

   
   for(int i=0;i<chunks.size();i++){
      threads.push_back(thread(bubble_sort,ref(chunks[i])));     //pass n chunks through n thread
   }
   for(int i=0;i<chunks.size();i++){
      threads[i].join();                                    //pauses until thread[i] finish its operation 
   }
   return chunks;                                       
}


//purposefully using slow sorting algorithm 

void bubble_sort(vector<float> &data){

    int i, j;
    int n_rows=max_data;
    float temp_data;
    for (i = 0; i < data.size()-1; i++){
        for (j = 0; j < data.size()-i-1; j++){
            if (data[j]<data[j+1]){
                  temp_data=data[j];
                  data[j]=data[j+1];
                  data[j+1]=temp_data;             
                }            
            }
    }
 
}


//merges chunks and returns sorted chunks
vector<float> merge_sorted_chunks(vector<vector<float>> sorted_chunks){
   
   vector<float> sorted;

   
   int min_value_chunk_index;                 
   bool sort_completed=false;
   
   while(sort_completed==false){
      float min_value=90; //latitude range from -90 to 90
      sort_completed=true;
      for(int i=0;i<sorted_chunks.size();i++){
      
         if (sorted_chunks[i].back()<min_value && !sorted_chunks[i].empty()){
            sort_completed=false;
            min_value=sorted_chunks[i].back();
            min_value_chunk_index=i;
            
         } 
      }
      if(sort_completed==true) break;
      sorted.push_back(min_value);
      sorted_chunks[min_value_chunk_index].pop_back();
   }
   return sorted; 
}