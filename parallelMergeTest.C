/// \file
/// \ingroup tutorial_net
///
/// \macro_code
///
/// \author

#include <random>
#include <vector>

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
    // Give the array a random size
    // & fill it with random numbers
    std::random_device rd;
    std::mt19937 mt(rd());

    // The size of each event's output data is between 2 and 3 MB
    std::uniform_real_distribution<double> dist_N(262144., 393216.);
    const int N {(int) dist_N(mt)};
    std::uniform_real_distribution<double> dist(0.0, 100.0);

    TFile *file = TFile::Open("toto.root?pmerge=localhost:1095","RECREATE");
   
    //int N = 20;

    TH1D *h = new TH1D("name", "title", N, 0, N);
    for (int i = 0; i < N; i++)
        h->Fill(i, dist(mt));
   
    Info("ROOT SENDER", "Size of data array: %d", N);
    for (int i = 1; i <= 20; i++)
        cout << h->GetBinContent(i) << "\t";
    cout << endl;

    file->Write();

    delete file;
}
