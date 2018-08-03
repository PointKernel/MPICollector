/// \file
/// \ingroup tutorial_net
///
/// \macro_code
///
/// \author

#include "TMessage.h"
#include "TBenchmark.h"
#include "TSocket.h"
#include "TH2.h"
#include "TTree.h"
#include "TFile.h"
#include "TRandom.h"
#include "TError.h"

const int NUM_EVENTS = 10;

using namespace std;

void parallelMergeTest()
{
   TFile *file = TFile::Open("toto.root?pmerge=localhost:1095","RECREATE");
   
   TH1D *h = new TH1D("name", "title", 20, 0, 20);
   for (int i = 0; i < 20; i++)
       h->Fill(i, i+0.5);

   for (int i = 1; i <= 20; i++)
       cout << h->GetBinContent(i) << "\t";
   cout << endl;

   file->Write();

   delete file;
}
