#ifndef _MASTERIO_
#define _MASTERIO_

enum class MasterIOStatus {UNLOCKED, LOCKED};

struct BufferSizes {
    int name_length;
    long data_length;
};

class MasterIO {
    public:
        MasterIO(const int n):
            num{0},
            total{n},
            master_io_status{MasterIOStatus::UNLOCKED}
        {}

        ~MasterIO() = default;

        int getTotal() const {return this->total;}
        int getNum() const {return this->num;}
        MasterIOStatus& getMasterIOStatus() {return this->master_io_status;}

        void setMasterIOStatus(const MasterIOStatus& s) {this->master_io_status = s;}
        void setNumIncrement() {this->num += 1;}
        
        void Write() {} 

    private:
        int num;
        const int total;
        MasterIOStatus master_io_status;
};

#endif
