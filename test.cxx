#include <iostream>
#include <algorithm>
#include <random>

#include "masterio.hxx"
#include "ParallelFileMerger.hxx"

#include <mpi.h>

const int TAG_DATA = 97;
const int TAG_REQUEST = 98;
const int TAG_FEEDBACK = 99;

const int EVENTS_PER_NODE = 5;
const int MAX_TOTAL = 5;

using namespace std;

int main(int argc, char** argv) {
    if (argc < 3) {
        cout << "EVENTS_PER_NODE and TOTAL_EVENTS not specified" << endl;
        exit(1);
    }  //otherwise continue on our merry way....
             
    const unsigned EVENTS_PER_NODE = atoi(argv[1]);
    const unsigned MAX_TOTAL = atoi(argv[2]);
    
    int nprocs, id;
    MPI_Init(&argc, &argv);

    // Get the world size & global rank
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &id);
    
    // user-defined mpi data type
    MPI_Datatype masterstat_type;
    MPI_Type_contiguous(sizeof(MasterIOStatus), MPI_BYTE, &masterstat_type);
    MPI_Type_commit(&masterstat_type);

    MPI_Datatype buffersizes_type;
    MPI_Type_contiguous(sizeof(BufferSizes), MPI_BYTE, &buffersizes_type);
    MPI_Type_commit(&buffersizes_type);

    // if master IO
    if ( id == 0 ) {
        string name = "MASTER IO: ";
        name += to_string(id);
        const char * rankID = name.c_str();

        bool cache = false;
        string msg  = "PID ";
        msg += to_string(getpid());
        Info(rankID, msg.c_str());

        THashTable mergers;
        MasterIO master_io(MAX_TOTAL);
            
        int clientId = 0;

        Info(rankID, "Ready to receive data...");
        // Loop to collect data from all ipcwriters
        while ( master_io.getNum() < master_io.getTotal() ) {
            MPI_Status recv_status;
            BufferSizes sizes;
            MPI_Recv(&sizes, 1, buffersizes_type, MPI_ANY_SOURCE, TAG_REQUEST, MPI_COMM_WORLD, &recv_status);
            msg = "\nLength of file name: ";
            msg += to_string(sizes.name_length);
            msg += "\nLength of data: ";
            msg += to_string(sizes.data_length);
            Info(rankID, msg.c_str());

            Info(rankID, "Send master IO status");
            MPI_Send(&master_io.getMasterIOStatus(), 1, masterstat_type, recv_status.MPI_SOURCE, TAG_FEEDBACK, MPI_COMM_WORLD);

            
            if (master_io.getMasterIOStatus() == MasterIOStatus::UNLOCKED) {
                master_io.setMasterIOStatus(MasterIOStatus::LOCKED);
                master_io.setNumIncrement();
        
                char *name = new char[sizes.name_length+1];
                char *data = new char[sizes.data_length+1];
                MPI_Recv(name, sizes.name_length, MPI_CHAR, recv_status.MPI_SOURCE, TAG_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Recv(data, sizes.data_length, MPI_CHAR, recv_status.MPI_SOURCE, TAG_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                name[sizes.name_length] = '\0';
                data[sizes.data_length] = '\0';

                TString filename(name);

                TMemFile *transient = new TMemFile(filename,data,sizes.data_length+1,"UPDATE"); // UPDATE because we need to remove the TTree after merging them.
             
                TH1D *h = (TH1D *)transient->Get("name");
                for (int i = 1; i <= 20; i++)
                    cout << h->GetBinContent(i) << "\t";
                cout << endl;

                ParallelFileMerger *info = (ParallelFileMerger*)mergers.FindObject(filename);
                if (!info) {
                    info = new ParallelFileMerger(filename,cache);
                    mergers.Add(info);
                }

                info->InitialMerge(transient);
                info->RegisterClient(clientId, transient);
                Info(rankID, "Merging input from event %d", clientId);
                info->Merge();
                clientId++;
                transient = 0;

                delete name;
                delete data;
                master_io.setMasterIOStatus(MasterIOStatus::UNLOCKED);
                Info(rankID, "Merge done");
          }
       }    // while
       mergers.Delete();
       Info(rankID, "ALL DONE");
    }   // rank 0
    else {

        pid_t fpid = fork();

        if (fpid == 0) {
            string name = "ROOT SENDER: ";
            name += to_string(id);
            const char * rankID = name.c_str();

            string msg  = "PID ";
            msg += to_string(getpid());
            Info(rankID, msg.c_str());
            for (unsigned i = 0; i < EVENTS_PER_NODE; ++i) {
                // Give the array a random size
                // & fill it with random numbers
                std::random_device rd;
                std::mt19937 mt(rd());

                // The size of each event's output data is between 2 and 3 MB
                std::uniform_real_distribution<double> dist(180.0, 300.0);
                int time = (int) dist(mt);
                sleep(time);
                system("root -q -b ../parallelMergeTest.C");
            }
            Info(rankID, "ALL DONE");
            return 0;
        }
        else {
            string name = "ROOT RECEIVER: ";
            name += to_string(id);
            const char * rankID = name.c_str();

            string msg  = "PID ";
            msg += to_string(getpid());
            Info(rankID, msg.c_str());
            
            TServerSocket *ss = new TServerSocket(1095, kTRUE, 100);
            
            if (!ss->IsValid())
                return 1;

            TMonitor *mon = new TMonitor;

            mon->Add(ss);

            UInt_t eventIndex = 0;

            THashTable mergers;

            enum StatusKind {
                kStartConnection = 0,
                kProtocol = 1,
                kProtocolVersion = 1
            };
            Info(rankID, "Ready to accept connections");
           
            while (1) {
                TMessage *mess;
                TSocket  *s;
                
                s = mon->Select();

                if (s->IsA() == TServerSocket::Class()) {
                    if (eventIndex >= EVENTS_PER_NODE) {
                        Info(rankID, "Only accept %d events per node", EVENTS_PER_NODE);
                        mon->Remove(ss);
                        ss->Close();
                    } else {
                        TSocket *client = ((TServerSocket *)s)->Accept();
                        client->Send(eventIndex, kStartConnection);
                        client->Send(kProtocolVersion, kProtocol);
                        ++eventIndex;
                        mon->Add(client);
                    }
                    continue;
                }

                s->Recv(mess);

                if (mess==0)
                    Error(rankID, "The client did not send a message\n");
                else if (mess->What() == kMESS_STRING) {
                    char str[64];
                    mess->ReadString(str, 64);
                    msg = "Event ";
                    msg += to_string(eventIndex);
                    msg += ": ";
                    msg += str;
                    Info(rankID, msg.c_str());

                    mon->Remove(s);
                    msg = "Event ";
                    msg += to_string(eventIndex);
                    msg += ": bytes recv = ";
                    msg += to_string(s->GetBytesRecv());
                    msg += ", bytes sent = ";
                    msg += to_string(s->GetBytesSent());
                    Info(rankID, msg.c_str());
                    s->Close();
                    
                    if (mon->GetActive() == 0 || eventIndex >= EVENTS_PER_NODE) {
                    //if (eventIndex >= EVENTS_PER_NODE) {
                        Info(rankID, "No more active clients... stopping");
                        break;
                    }
                } else if (mess->What() == kMESS_ANY) {
                    Long64_t length;
                    TString filename;
                    Int_t clientId;
                    mess->ReadInt(clientId);
                    mess->ReadTString(filename);
                    mess->ReadLong64(length); // '*mess >> length;' is broken in CINT for Long64_t.
                    
                    auto start = std::chrono::high_resolution_clock::now();
                    
                    BufferSizes sizes;
                    sizes.name_length = filename.Length();
                    sizes.data_length = length;
                 
                    MPI_Send(&sizes, 1, buffersizes_type, 0, TAG_REQUEST, MPI_COMM_WORLD);
                    MasterIOStatus master_io_stat;
                    MPI_Recv(&master_io_stat, 1, masterstat_type, 0, TAG_FEEDBACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    string msg = "Receive master IO status: ";
                    if ( master_io_stat == MasterIOStatus::UNLOCKED)
                        msg += " UNLOCKED";
                    else
                        msg += "LOCKED";
                    Info(rankID, msg.c_str());
                 
                    while(master_io_stat != MasterIOStatus::UNLOCKED){
                        usleep(10);
                        MPI_Send(&sizes, 1, buffersizes_type, 0, TAG_REQUEST, MPI_COMM_WORLD);
                        Info(rankID, "Waiting for master IO status...");
                        MPI_Recv(&master_io_stat, 1, masterstat_type, 0, TAG_FEEDBACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        msg = "Receive master IO status: ";
                        if (master_io_stat == MasterIOStatus::UNLOCKED)
                            msg += " UNLOCKED";
                        else
                            msg += "LOCKED";
                        Info(rankID, msg.c_str());
                    }
                    char *data_buffer = mess->Buffer() + mess->Length();
                    MPI_Send(filename.Data(), sizes.name_length, MPI_CHAR, 0, TAG_DATA, MPI_COMM_WORLD);
                    MPI_Send(data_buffer, sizes.data_length, MPI_CHAR, 0, TAG_DATA, MPI_COMM_WORLD);

                    mess->SetBufferOffset(mess->Length()+length);
                    data_buffer = 0;
                    
                    auto end = std::chrono::high_resolution_clock::now();
                    double time = std::chrono::duration_cast<std::chrono::duration<double>> (end - start).count();
                    msg = "Data transfer overhead: ";
                    msg += to_string(time);
                    Info(rankID, msg.c_str());

                } else if (mess->What() == kMESS_OBJECT) {
                    msg = "got object of class: ";
                    msg += mess->GetClass()->GetName();
                    Info(rankID, msg.c_str());
                } else
                    Info(rankID, "Unexpected message");

                delete mess;
            }    //while
            
            delete mon;
            delete ss;
            
            Info(rankID, "ALL DONE");
        }   // else -- pid of root receiver

        Info("OTHER RANKS", "ALL DONE");
    }   // other ranks

   return 0;
}
