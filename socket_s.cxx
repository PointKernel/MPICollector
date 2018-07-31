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

const int MAX_FILES = 10;

using namespace std;

int main(int argc, char** argv) {
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
        MasterIO master_io(1);

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

                const Float_t clientThreshold = 0.75; // control how often the histogram are merged.  Here as soon as half the clients have reported.

                ParallelFileMerger *info = (ParallelFileMerger*)mergers.FindObject(filename);
                if (!info) {
                    info = new ParallelFileMerger(filename,cache);
                    mergers.Add(info);
                }

                if (R__NeedInitialMerge(transient)) {
                    info->InitialMerge(transient);
                }
                yunsong::DEBUG_MSG("after initialmerge");
                info->RegisterClient(clientId,transient);
                yunsong::DEBUG_MSG("registerclient");
                if (info->NeedMerge(clientThreshold)) {
                    // Enough clients reported.
                    Info("fastMergeServerHist","Merging input from %ld clients (%d)",info->fClients.size(),clientId);
                    info->Merge();
                }
                transient = 0;
                ++clientId;

                delete name;
                delete data;
          }

       }    // while

       TIter next(&mergers);
       ParallelFileMerger *info;
       while ( (info = (ParallelFileMerger*)next()) ) {
          if (info->NeedFinalMerge())
          {
             info->Merge();
          }
       }
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

       UInt_t clientCount = 0;
       UInt_t clientIndex = 0;

       THashTable mergers;

       enum StatusKind {
          kStartConnection = 0,
          kProtocol = 1,

          kProtocolVersion = 1
       };

       printf("fastMergeServerHist ready to accept connections\n");
       while (1) {
           TMessage *mess;
           TSocket  *s;

          // NOTE: this needs to be update to handle the case where the client
          // dies.
          s = mon->Select();

          if (s->IsA() == TServerSocket::Class()) {
             if (clientCount > 100) {
                printf("only accept 100 clients connections\n");
                mon->Remove(ss);
                ss->Close();
             } else {
                TSocket *client = ((TServerSocket *)s)->Accept();
                client->Send(clientIndex, kStartConnection);
                client->Send(kProtocolVersion, kProtocol);
                ++clientCount;
                ++clientIndex;
                mon->Add(client);
                printf("Accept %d connections\n",clientCount);
             }
             continue;
          }

          s->Recv(mess);

          if (mess==0) {
             Error("fastMergeServer","The client did not send a message\n");
          } else if (mess->What() == kMESS_STRING) {
             char str[64];
             mess->ReadString(str, 64);
             printf("Client %d: %s\n", clientCount, str);
             mon->Remove(s);
             printf("Client %d: bytes recv = %d, bytes sent = %d\n", clientCount, s->GetBytesRecv(),
                    s->GetBytesSent());
             s->Close();
             --clientCount;
             if (mon->GetActive() == 0 || clientCount == 0) {
                printf("No more active clients... stopping\n");
                break;
             }
          } else if (mess->What() == kMESS_ANY) {

             Long64_t length;
             TString filename;
             Int_t clientId;
             mess->ReadInt(clientId);
             string msg;
             msg = "clientId: ";
             msg += to_string(clientId);
             yunsong::DEBUG_MSG(msg);
             mess->ReadTString(filename);
             msg = "filename: ";
             msg += filename;
             yunsong::DEBUG_MSG(msg);
             mess->ReadLong64(length); // '*mess >> length;' is broken in CINT for Long64_t.
             msg = "length: ";
             msg += to_string(length);
             yunsong::DEBUG_MSG(msg);
             
             BufferSizes sizes;
             sizes.name_length = filename.Length();
             sizes.data_length = length;

             msg = "filename length: ";
             msg += to_string(sizes.name_length);
             yunsong::DEBUG_MSG(msg);
             
             MPI_Send(&sizes, 1, buffersizes_type, 0, TAG_REQUEST, MPI_COMM_WORLD);
             MasterIOStatus master_io_stat;
             MPI_Recv(&master_io_stat, 1, masterstat_type, 0, TAG_FEEDBACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
             msg = "Receive master IO status: ";
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
             sleep(5);
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
