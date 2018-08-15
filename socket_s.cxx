#include <iostream>
#include <algorithm>
#include <random>

#include "wys.hxx"
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

    if ( id == 0 ) {
        bool cache = false;
        string msg  = "I'm the master IO process: ";
        msg += to_string(getpid());
        yunsong::DEBUG_MSG(msg);

        THashTable mergers;
        MasterIO master_io(MAX_TOTAL);

        int clientId = 0;

        yunsong::DEBUG_MSG("MasterIO Ready to receive data...");
        // Loop to collect data from all ipcwriters
        while ( master_io.getNum() < master_io.getTotal() ) {
            MPI_Status recv_status;
            BufferSizes sizes;
            MPI_Recv(&sizes, 1, buffersizes_type, MPI_ANY_SOURCE, TAG_REQUEST, MPI_COMM_WORLD, &recv_status);
            msg = "Length of file name: ";
            msg += to_string(sizes.name_length);
            msg += "\nLength of data: ";
            msg += to_string(sizes.data_length);
            yunsong::DEBUG_MSG(msg);

            yunsong::DEBUG_MSG("Send master IO status");
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
                info->RegisterClient(clientId,transient);
                Info("MergeServer","Merging input from event %d",clientId);
                info->Merge();
                transient = 0;
                ++clientId;

                delete name;
                delete data;
                master_io.setMasterIOStatus(MasterIOStatus::UNLOCKED);
                Info("MergeServer","Done");
          }
       }    // while
       mergers.Delete();
    }   // rank 0
    else {
       // Open a server socket looking for connections on a named service or
       // on a specified port.
       //TServerSocket *ss = new TServerSocket("rootserv", kTRUE);
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

       printf("ParallelMergeServer ready to accept connections\n");
       while (1) {
           TMessage *mess;
           TSocket  *s;

          // NOTE: this needs to be update to handle the case where the client
          // dies.
          s = mon->Select();

          if (s->IsA() == TServerSocket::Class()) {
             if (eventCount >= EVENTS_PER_NODE) {
                 Info("ParallelMergeServer","Only accept %d events per node", EVENTS_PER_NODE);
                 mon->Remove(ss);
                 ss->Close();
             } else {
                TSocket *client = ((TServerSocket *)s)->Accept();
                client->Send(eventIndex, kStartConnection);
                client->Send(kProtocolVersion, kProtocol);
                ++eventIndex;
                mon->Add(client);
                printf("######### Accept event %d #########\n",eventCount);
             }
             continue;
          }

          s->Recv(mess);

          if (mess==0) {
             Error("ParallelMergeServer","The client did not send a message\n");
          } else if (mess->What() == kMESS_STRING) {
             char str[64];
             mess->ReadString(str, 64);
             printf("Event %d: %s\n", eventCount, str);
             mon->Remove(s);
             printf("Client %d: bytes recv = %d, bytes sent = %d\n", eventCount, s->GetBytesRecv(),
                    s->GetBytesSent());
             s->Close();
             if (mon->GetActive() == 0 && eventCount >= EVENTS_PER_NODE) {
                printf("No more active clients... stopping\n");
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
             yunsong::DEBUG_MSG(msg);
             
             while(master_io_stat != MasterIOStatus::UNLOCKED){
                 usleep(10);
                 MPI_Send(&sizes, 1, buffersizes_type, 0, TAG_REQUEST, MPI_COMM_WORLD);
                 yunsong::DEBUG_MSG("Waiting for status...");
                 MPI_Recv(&master_io_stat, 1, masterstat_type, 0, TAG_FEEDBACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                 msg = "Receive master IO status: ";
                 if (master_io_stat == MasterIOStatus::UNLOCKED)
                     msg += " UNLOCKED";
                 else
                     msg += "LOCKED";
                 yunsong::DEBUG_MSG(msg);
             }
             char *data_buffer = mess->Buffer() + mess->Length();
             MPI_Send(filename.Data(), sizes.name_length, MPI_CHAR, 0, TAG_DATA, MPI_COMM_WORLD);
             MPI_Send(data_buffer, sizes.data_length, MPI_CHAR, 0, TAG_DATA, MPI_COMM_WORLD);

             mess->SetBufferOffset(mess->Length()+length);
             data_buffer = 0;
             ++eventCount;
          } else if (mess->What() == kMESS_OBJECT) {
             printf("got object of class: %s\n", mess->GetClass()->GetName());
          } else {
             printf("*** Unexpected message ***\n");
          }

          delete mess;
       }    //while
       delete mon;
       delete ss;
    }   // other ranks

   return 0;
}
