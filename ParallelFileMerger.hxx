#ifndef _PARALLELFILEMERGER_
#define _PARALLELFILEMERGER_

#include "TMessage.h"
#include "TBenchmark.h"
#include "TSocket.h"
#include "TH2.h"
#include "TTree.h"
#include "TMemFile.h"
#include "TRandom.h"
#include "TError.h"
#include "TFileMerger.h"

#include "TServerSocket.h"
#include "TPad.h"
#include "TCanvas.h"
#include "TMonitor.h"

#include "TFileCacheWrite.h"
#include "TSystem.h"
#include "THashTable.h"

#include "TMath.h"
#include "TTimeStamp.h"

const int kIncremental = 0;
const int kReplaceImmediately = 1;
const int kReplaceWait = 2;

#include "TKey.h"
#include "TROOT.h"

static Bool_t R__NeedInitialMerge(TDirectory *dir)
{

   if (dir==0) return kFALSE;

   TIter nextkey(dir->GetListOfKeys());
   TKey *key;
   while( (key = (TKey*)nextkey()) ) {
      TClass *cl = TClass::GetClass(key->GetClassName());
      if (cl->InheritsFrom(TDirectory::Class())) {
         TDirectory *subdir = (TDirectory *)dir->GetList()->FindObject(key->GetName());
         if (!subdir) {
            subdir = (TDirectory *)key->ReadObj();
         }
         if (R__NeedInitialMerge(subdir)) {
            return kTRUE;
         }
      } else {
         if (0 != cl->GetResetAfterMerge()) {
            return kTRUE;
         }
      }
   }
   return kFALSE;
}

static void R__DeleteObject(TDirectory *dir, Bool_t withReset)
{
   if (dir==0) return;

   TIter nextkey(dir->GetListOfKeys());
   TKey *key;
   while( (key = (TKey*)nextkey()) ) {
      TClass *cl = TClass::GetClass(key->GetClassName());
      if (cl->InheritsFrom(TDirectory::Class())) {
         TDirectory *subdir = (TDirectory *)dir->GetList()->FindObject(key->GetName());
         if (!subdir) {
            subdir = (TDirectory *)key->ReadObj();
         }
         R__DeleteObject(subdir,withReset);
      } else {
         Bool_t todelete = kFALSE;
         if (withReset) {
            todelete = (0 != cl->GetResetAfterMerge());
         } else {
            todelete = (0 ==  cl->GetResetAfterMerge());
         }
         if (todelete) {
            key->Delete();
            dir->GetListOfKeys()->Remove(key);
            delete key;
         }
      }
   }
}

static void R__MigrateKey(TDirectory *destination, TDirectory *source)
{
   if (destination==0 || source==0) return;

   TIter nextkey(source->GetListOfKeys());
   TKey *key;
   while( (key = (TKey*)nextkey()) ) {
      TClass *cl = TClass::GetClass(key->GetClassName());
      if (cl->InheritsFrom(TDirectory::Class())) {
         TDirectory *source_subdir = (TDirectory *)source->GetList()->FindObject(key->GetName());
         if (!source_subdir) {
            source_subdir = (TDirectory *)key->ReadObj();
         }
         TDirectory *destination_subdir = destination->GetDirectory(key->GetName());
         if (!destination_subdir) {
            destination_subdir = destination->mkdir(key->GetName());
         }
         R__MigrateKey(destination,source);
      } else {
         TKey *oldkey = destination->GetKey(key->GetName());
         if (oldkey) {
            oldkey->Delete();
            delete oldkey;
         }
         TKey *newkey = new TKey(destination,*key,0 /* pidoffset */); // a priori the file are from the same client ..
         destination->GetFile()->SumBuffer(newkey->GetObjlen());
         newkey->WriteFile(0);
         if (destination->GetFile()->TestBit(TFile::kWriteError)) {
            return;
         }
      }
   }
   destination->SaveSelf();
}

struct ClientInfo
{
   TFile      *fFile;      // This object does *not* own the file, it will be own by the owner of the ClientInfo.
   TString    fLocalName;
   UInt_t     fContactsCount;
   TTimeStamp fLastContact;
   Double_t   fTimeSincePrevContact;

   ClientInfo() : fFile(0), fLocalName(), fContactsCount(0), fTimeSincePrevContact(0) {}
   ClientInfo(const char *filename, UInt_t clientId) : fFile(0), fContactsCount(0), fTimeSincePrevContact(0) {
      fLocalName.Form("%s-%d-%d",filename,clientId,gSystem->GetPid());
   }

   void Set(TFile *file)
   {
      // Register the new file as coming from this client.
      if (file != fFile) {
         // We need to keep any of the keys from the previous file that
         // are not in the new file.
         if (fFile) {
            R__MigrateKey(fFile,file);
            // delete the previous memory file (if any)
            delete file;
         } else {
            fFile = file;
         }
      }
      TTimeStamp now;
      fTimeSincePrevContact = now.AsDouble() - fLastContact.AsDouble();
      fLastContact = now;
      ++fContactsCount;
   }
};

struct ParallelFileMerger : public TObject
{
   typedef std::vector<ClientInfo> ClientColl_t;

   TString       fFilename;
   TBits         fClientsContact;       //
   UInt_t        fNClientsContact;      //
   ClientColl_t  fClients;
   TTimeStamp    fLastMerge;
   TFileMerger   fMerger;

   ParallelFileMerger() = default;

   ParallelFileMerger(const char *filename, Bool_t writeCache = kFALSE) : fFilename(filename), fNClientsContact(0), fMerger(kFALSE,kTRUE)
   {
      // Default constructor.

      fMerger.SetPrintLevel(0);
      fMerger.OutputFile(filename,"RECREATE");
      if (writeCache) new TFileCacheWrite(fMerger.GetOutputFile(),32*1024*1024);
   }

   ~ParallelFileMerger()
   {
      // Destructor.
      for( ClientColl_t::iterator iter = fClients.begin();
          iter != fClients.end();
          ++iter)
      {
         delete iter->fFile;
      }
      ROOT::CallRecursiveRemoveIfNeeded(*this);
   }

   ULong_t  Hash() const
   {
      // Return hash value for this object.
      return fFilename.Hash();
   }

   const char *GetName() const
   {
      // Return the name of the object which is the name of the output file.
      return fFilename;
   }

   Bool_t InitialMerge(TFile *input)
   {
      // Initial merge of the input to copy the resetable object (TTree) into the output
      // and remove them from the input file.

      fMerger.AddFile(input);

      Bool_t result = fMerger.PartialMerge(TFileMerger::kIncremental | TFileMerger::kResetable);

      R__DeleteObject(input,kTRUE);
      return result;
   }

   Bool_t Merge()
   {
      // Merge the current inputs into the output file.

      R__DeleteObject(fMerger.GetOutputFile(),kFALSE); // Remove object that can *not* be incrementally merge and will *not* be reset by the client code.
      for(unsigned int f = 0 ; f < fClients.size(); ++f) {
         fMerger.AddFile(fClients[f].fFile);
      }
      Bool_t result = fMerger.PartialMerge(TFileMerger::kAllIncremental);

      // Remove any 'resetable' object (like TTree) from the input file so that they will not
      // be re-merged.  Keep only the object that always need to be re-merged (Histograms).
      for(unsigned int f = 0 ; f < fClients.size(); ++f) {
         if (fClients[f].fFile) {
            R__DeleteObject(fClients[f].fFile,kTRUE);
         } else {
            // We back up the file (probably due to memory constraint)
            TFile *file = TFile::Open(fClients[f].fLocalName,"UPDATE");
            R__DeleteObject(file,kTRUE); // Remove object that can be incrementally merge and will be reset by the client code.
            file->Write();
            delete file;
         }
      }
      fLastMerge = TTimeStamp();
      fNClientsContact = 0;
      fClientsContact.Clear();

      return result;
   }

   Bool_t NeedFinalMerge()
   {
      // Return true, if there is any data that has not been merged.

      return fClientsContact.CountBits() > 0;
   }

   Bool_t NeedMerge(Float_t clientThreshold)
   {
      // Return true, if enough client have reported

      if (fClients.size()==0) {
         return kFALSE;
      }

      // Calculate average and rms of the time between the last 2 contacts.
      Double_t sum = 0;
      Double_t sum2 = 0;
      for(unsigned int c = 0 ; c < fClients.size(); ++c) {
         sum += fClients[c].fTimeSincePrevContact;
         sum2 += fClients[c].fTimeSincePrevContact*fClients[c].fTimeSincePrevContact;
      }
      Double_t avg = sum / fClients.size();
      Double_t sigma = sum2 ? TMath::Sqrt( sum2 / fClients.size() - avg*avg) : 0;
      Double_t target = avg + 2*sigma;
      TTimeStamp now;
      if ( (now.AsDouble() - fLastMerge.AsDouble()) > target) {
//         Float_t cut = clientThreshold * fClients.size();
//         if (!(fClientsContact.CountBits() > cut )) {
//            for(unsigned int c = 0 ; c < fClients.size(); ++c) {
//               fprintf(stderr,"%d:%f ",c,fClients[c].fTimeSincePrevContact);
//            }
//            fprintf(stderr,"merge:%f avg:%f target:%f\n",(now.AsDouble() - fLastMerge.AsDouble()),avg,target);
//         }
         return kTRUE;
      }
      Float_t cut = clientThreshold * fClients.size();
      return fClientsContact.CountBits() > cut  || fNClientsContact > 2*cut;
   }

   void RegisterClient(UInt_t clientId, TFile *file)
   {
      // Register that a client has sent a file.

      ++fNClientsContact;
      fClientsContact.SetBitNumber(clientId);
      if (fClients.size() < clientId+1) {
         fClients.push_back( ClientInfo(fFilename,clientId) );
      }
      fClients[clientId].Set(file);
   }

   ClassDef(ParallelFileMerger,0);
};

#endif
