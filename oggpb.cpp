// $ protoc foo.pb --cpp_out=.
// $ g++ -o oggpb foo.pb.pb.cc oggpb.cpp $(pkg-config --cflags --libs oggz protobuf)

#include "foo.pb.pb.h"

#include "oggz/oggz.h"

#include <iostream>
#include <cstdlib>
#include <cassert>
#include <vector>

using namespace std;

static int
oggpb_write(const char* filename)
{
    OGGZ* oggz = oggz_open (filename, OGGZ_WRITE);

    // simulate writing a few streams with different time granule
    // series.

    const int nloops = 10;
    const int nstreams = 3;
    std::vector<int64_t> tnow{10,100,1000}, dt{1000,100,10};
    std::vector<size_t> nnumbers{1000,100,10};
    std::vector<size_t> pkt_count{0,0,0};
    std::vector<size_t> stream_size{0,0,0};
    std::vector<std::string> stream_names{"foo","bar","baz"};

    size_t total_size = 0;

    for (int loop=0; loop<nloops+1; ++loop) {

        for (int istream=0; istream<nstreams; ++istream) {

            const int npkts = 2 + 1<<(istream+1);


            if (loop == 0) {    // BOS
                ogg_packet op;
                unsigned char buf[1];
                buf[0] = 'A' + (int)istream;
                op.packet = buf;
                op.bytes = 1;
                total_size += 1;
                stream_size[istream] += 1;

                op.granulepos = 0;
                op.packetno = pkt_count[istream]++;
                op.b_o_s = 1;
                op.e_o_s = 0;
                const int flags = OGGZ_FLUSH_AFTER;
                std::cout << "write bos" << istream+1 << " " << op.packetno
                          << " @ " << op.granulepos
                          << " " << std::hex << flags << std::dec
                          << "\n";
                int rc = oggz_write_feed(oggz, &op, istream+1, flags, NULL);
                assert(rc == 0);
                while (oggz_write (oggz, 32) > 0);
                continue;
            }

            if (loop == nloops) { // EOS
                ogg_packet op;
                unsigned char buf[1];
                buf[0] = 'A' + (int)istream;
                op.packet = buf;
                op.bytes = 1;
                total_size += 1;
                stream_size[istream] += 1;

                op.granulepos = tnow[istream] + (pkt_count[istream]) * dt[istream];
                op.packetno = pkt_count[istream]++;
                op.b_o_s = 0;
                op.e_o_s = 1;
                const int flags = OGGZ_FLUSH_AFTER;
                std::cout << "write eos" << istream+1 << " " << op.packetno
                          << " @ " << op.granulepos
                          << " " << std::hex << flags << std::dec
                          << "\n";
                int rc = oggz_write_feed(oggz, &op, istream+1, flags, NULL);
                assert(rc == 0);

                while (oggz_write (oggz, 32) > 0);
                continue;
            }

            std::vector<ogg_packet> garbage(npkts);

            for (int ipkt=0; ipkt<npkts; ++ipkt) {
                ogg_packet& op = garbage[ipkt];

                foo::Foo f;
                f.set_text(stream_names[istream]);
                for (int n=0; n<nnumbers[istream]; ++n) {
                    f.add_numbers(n);
                }

                size_t siz = f.ByteSize();
                total_size += siz;
                stream_size[istream] += siz;
                unsigned char* buf = (unsigned char*)malloc(siz);
                assert(buf);
                f.SerializeToArray(buf,siz);
                op.packet = buf;
                op.bytes = siz;
                op.granulepos = tnow[istream] + dt[istream]*pkt_count[istream];
                op.packetno = pkt_count[istream]++;
                op.b_o_s = 0;
                op.e_o_s = 0;
            
                int flags = 0;
                if (ipkt == 0) {
                    flags |= OGGZ_FLUSH_BEFORE;
                }
                if (ipkt == npkts-1) {
                    flags |= OGGZ_FLUSH_AFTER;
                }
                //flags |= OGGZ_FLUSH_AFTER;
                std::cout << "write stream" << istream+1 << " " << op.packetno
                          << " @ " << op.granulepos
                          << " " << std::hex << flags << std::dec
                          << " " << siz << " B"
                          << "\n";
                int rc = oggz_write_feed(oggz, &op, istream+1, flags, NULL);
                if (rc != 0) {
                    std::cout << "write failed: " << rc << "\n";
                }
                while (oggz_write (oggz, 32) > 0);
            } // packet
        } // stream
    
        // long n = 0;
        // while ((n = oggz_write (oggz, 32)) > 0);

    } // loop
    long n = 0;
    while ((n = oggz_write (oggz, 32)) > 0);

    oggz_close(oggz);

    std::cout << "sizes:";
    for (auto n : stream_size) {
        std::cout << " " << n;
    }
    std::cout << " total: " << total_size << " B\n";

    std::cout << "package counts:";
    for (auto n : pkt_count) {
        std::cout << " " << n;
    }
    std::cout << std::endl;

    return 0;
}

static int
read_packet (OGGZ * oggz, oggz_packet * zp, long serialno, void * user_data)
{
    ogg_packet * op = &zp->op;

    std::cout << "read stream " << serialno
              << " pkt# " << op->packetno
              << " @ " << op->granulepos;
    if (op->b_o_s) 
        std::cout << " BOS";
    if (op->e_o_s)
        std::cout << " EOS";
    if (op->bytes == 1 ) {
        std::cout << " " << op->packet[0];
    }
    else {
        foo::Foo f;
        f.ParseFromArray(op->packet,op->bytes);
        std::cout << " " << f.text() << " " << f.numbers_size();
    }
    std::cout << std::endl;
    return 0;
}

static int
oggpb_read(const char* filename)
{
    OGGZ* oggz = oggz_open (filename, OGGZ_READ);
    assert(oggz);
    oggz_set_read_callback(oggz, -1, read_packet, NULL);
    oggz_run(oggz);
    oggz_close(oggz);
    return 0;
}

int main(int argc, char* argv[])
{
    const char* filename = "oggpb.ogp";
    oggpb_write(filename);
    oggpb_read(filename);
   
}
