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
        bool cache = false;
        string msg  = "pid: ";
        msg += to_string(getpid());
        Info("MASTER IO", msg.c_str());

        THashTable mergers;
        MasterIO master_io(MAX_TOTAL);
            
        int clientId = 0;

        Info("MASTER IO", "Ready to receive data...");
        // Loop to collect data from all ipcwriters
        while ( master_io.getNum() < master_io.getTotal() ) {
            MPI_Status recv_status;
            BufferSizes sizes;
            MPI_Recv(&sizes, 1, buffersizes_type, MPI_ANY_SOURCE, TAG_REQUEST, MPI_COMM_WORLD, &recv_status);
            msg = "\nLength of file name: ";
            msg += to_string(sizes.name_length);
            msg += "\nLength of data: ";
            msg += to_string(sizes.data_length);
            Info("MASTER IO", msg.c_str());

            Info("MASTER IO", "Send master IO status");
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
                Info("MASTER IO", "Merging input from event %d", clientId);
                info->Merge();
                clientId++;
                transient = 0;

                delete name;
                delete data;
                master_io.setMasterIOStatus(MasterIOStatus::UNLOCKED);
                Info("MergeServer", "Done");
          }
       }    // while
       mergers.Delete();
       Info("MASTER IO", "Done");
    }   // rank 0
    else {
       // Open a server socket looking for connections on a named service or
       // on a specified port.
       //TServerSocket *ss = new TServerSocket("rootserv", kTRUE);
       pid_t fpid = fork();

       if (fpid == 0) {
        string msg  = "pid: ";
        msg += to_string(getpid());
        Info("ROOT SENDER", msg.c_str());
        sleep(10);
        for (unsigned i = 0; i < EVENTS_PER_NODE; ++i) {
            sleep(5);
            system("root -q -b parallelMergeTest.C");
        }
        Info("parallelMergeTest", "DONE");
        return 0;
       }
       else {
        string msg  = "pid: ";
        msg += to_string(getpid());
        Info("ROOT RECEIVER", msg.c_str());

           TServerSocket *ss = new TServerSocket(1095, kTRUE, 100);
           if (!ss->IsValid()) {
              return 1;
           }

           TMonitor *mon = new TMonitor;

           mon->Add(ss);

           UInt_t eventCount = 0;
           UInt_t eventIndex = 0;

           THashTable mergers;

           enum StatusKind {
              kStartConnection = 0,
              kProtocol = 1,

              kProtocolVersion = 1
           };
           
           Info("ROOT RECEIVER", "Ready to accept connections");
           
           while (1) {
              TMessage *mess;
              TSocket  *s;

              // NOTE: this needs to be update to handle the case where the client
              // dies.
              s = mon->Select();

              if (s->IsA() == TServerSocket::Class()) {
                 if (eventCount >= EVENTS_PER_NODE) {
                     Info("ROOT RECEIVER", "Only accept %d events per node", EVENTS_PER_NODE);
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

              if (mess==0) {
                 Error("ParallelMergeServer","The client did not send a message\n");
              } else if (mess->What() == kMESS_STRING) {
                 char str[64];
                 mess->ReadString(str, 64);
                 msg = "Event ";
                 msg += to_string(eventCount);
                 msg += ": ";
                 msg += str;
                 Info("ROOT RECEIVER", msg.c_str());

                 mon->Remove(s);
                 msg = "Event ";
                 msg += to_string(eventCount);
                 msg += ": bytes recv = ";
                 msg += to_string(s->GetBytesRecv());
                 msg += ", bytes sent = ";
                 msg += to_string(s->GetBytesSent());
                 Info("ROOT RECEIVER", msg.c_str());

                 s->Close();
                 if (eventCount >= EVENTS_PER_NODE) {
                    Info("ROOT RECEIVER", "No more active clients... stopping");
                    break;
                 }
              } else if (mess->What() == kMESS_ANY) {
                 Long64_t length;
                 TString filename;
                 Int_t clientId;
                 mess->ReadInt(clientId);
                 mess->ReadTString(filename);
                 mess->ReadLong64(length); // '*mess >> length;' is broken in CINT for Long64_t.
                 
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
                 Info("MPI SENDER", msg.c_str());
                 
                 while(master_io_stat != MasterIOStatus::UNLOCKED){
                     usleep(10);
                     MPI_Send(&sizes, 1, buffersizes_type, 0, TAG_REQUEST, MPI_COMM_WORLD);
                     Info("MPI SENDER", "Waiting for status...");
                     MPI_Recv(&master_io_stat, 1, masterstat_type, 0, TAG_FEEDBACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                     msg = "Receive master IO status: ";
                     if (master_io_stat == MasterIOStatus::UNLOCKED)
                         msg += " UNLOCKED";
                     else
                         msg += "LOCKED";
                     Info("MPI SENDER", msg.c_str());
                 }
                 char *data_buffer = mess->Buffer() + mess->Length();
                 MPI_Send(filename.Data(), sizes.name_length, MPI_CHAR, 0, TAG_DATA, MPI_COMM_WORLD);
                 MPI_Send(data_buffer, sizes.data_length, MPI_CHAR, 0, TAG_DATA, MPI_COMM_WORLD);

                 mess->SetBufferOffset(mess->Length()+length);
                 data_buffer = 0;
                 ++eventCount;
              } else if (mess->What() == kMESS_OBJECT) {
                 msg = "got object of class: ";
                 msg += mess->GetClass()->GetName();
                 Info("ROOT RECEIVER", msg.c_str());
              } else
                 Info("ROOT RECEIVER", "Unexpected message");

              delete mess;
           }    //while
           delete mon;
           delete ss;

           Info("ROOT RECEIVER", "DONE");
        }   // else -- pid of root receiver

        Info("OTHER RANKS", "DONE");
    }   // other ranks

   return 0;
}
