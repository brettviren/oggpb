// $ protoc foo.pb --cpp_out=.
// $ g++ -o oggpb foo.pb.pb.cc oggpb.cpp $(pkg-config --cflags --libs oggz protobuf)

#include "foo.pb.pb.h"

#include "oggz/oggz.h"

#include <iostream>
#include <cstdlib>
#include <cassert>
#include <vector>
#include <sstream>

using namespace std;

static unsigned char*
WriteLEUint64(unsigned char* p, const ogg_uint64_t num)
{
  ogg_int32_t i;
  ogg_uint64_t n = num;
  assert(p);
  for (i=0; i<8; i++) {
    p[i] = (unsigned char)(n & 0xff);
    n >>= 8;
  }
  return p + 8;
}

static unsigned char*
WriteLEUint16(unsigned char* p, const ogg_uint16_t num)
{
  ogg_uint16_t n = num;
  assert(p);
  p[0] = (unsigned char)(n & 0xff);
  p[1] = (unsigned char)((n >> 8) & 0xff);
  return p + 2;
}

static void
oggpb_write_fishead(OGGZ* oggz, int streamnum)
{
    ogg_packet op;
    memset(&op, 0, sizeof(ogg_packet));
    const int fishead_size = 64; // v3
    op.packet = new unsigned char[fishead_size];
    memset(op.packet, 0, fishead_size);
    op.bytes = fishead_size;
    op.packetno = 0;
    op.b_o_s = 1;
    memcpy(op.packet, "fishead", 8);
    const uint16_t vmaj=3, vmin=0;
    memcpy(op.packet+8, &vmaj, sizeof(uint16_t));
    memcpy(op.packet+10, &vmin, sizeof(uint16_t));
    const uint64_t pdenom=1000000, bdenom=1000000;
    memcpy(op.packet+20, &pdenom, sizeof(uint64_t));
    memcpy(op.packet+36, &bdenom, sizeof(uint64_t));
    int rc = oggz_write_feed(oggz, &op, streamnum, OGGZ_FLUSH_AFTER, NULL);
    assert(rc == 0);
    while (oggz_write (oggz, 32) > 0);
}
static size_t
oggpb_write_fisbone(OGGZ* oggz, int fistream, int pktnum, int istream,
                     const std::string& name)
{
    // fisbone for our single Foo stream
    std::stringstream ss;
    ss << "Content-Type: protobuf/foo-Foo\r\n"
       << "Role: oggpb/" << name << "\r\n"
       << "Name: oggpb-" << name << "\r\n";
    std::string headers = ss.str();

    ogg_packet op;
    memset(&op, 0, sizeof(ogg_packet));
    const int fisbone_size = 52 + headers.size() + 1; // v3
    op.packet = new unsigned char[fisbone_size];
    memset(op.packet, 0, fisbone_size);
    op.bytes = fisbone_size;
    op.packetno = pktnum;
    memcpy(op.packet, "fisbone", 8);
    const uint32_t offset_to_msg_headers = 52;
    memcpy(op.packet+8, &offset_to_msg_headers, sizeof(uint32_t));
    memcpy(op.packet+12, &istream, sizeof(uint32_t));
    // skip header packets, gran numer, denom, offset, preroll, shift
    // memcpy(op.packet+16,...);
    memcpy(op.packet+52, headers.c_str(), headers.size()+1);

    int rc = oggz_write_feed(oggz, &op, fistream, OGGZ_FLUSH_AFTER, NULL);
    std::cerr << "write fis returns " << rc
              << " for:\n" << headers << std::endl;
    assert(rc == 0);
    while (oggz_write (oggz, 32) > 0);
    return fisbone_size;
}

static void
oggpb_write_fistail(OGGZ* oggz, int fistream, int pktnum)
{
    // skeleton eos
    ogg_packet op;
    memset(&op, 0, sizeof(ogg_packet));
    op.packetno = pktnum;
    op.e_o_s = 1;
    int rc = oggz_write_feed(oggz, &op, fistream, OGGZ_FLUSH_AFTER, NULL);
    std::cerr << "write fis eos returns " << rc << std::endl;
    assert(rc == 0);
    while (oggz_write (oggz, 32) > 0);
}

static size_t
oggpb_write_foohead(OGGZ* oggz, int streamnum)
{
    size_t stream_size = 0;

    ogg_packet op;
    memset(&op, 0, sizeof(ogg_packet));

    unsigned char buf[1];
    buf[0] = 'A' + (int)streamnum;
    op.packet = buf;
    op.bytes = 1;
    stream_size += 1;

    op.granulepos = 0;
    op.packetno = 0;
    op.b_o_s = 1;
    const int flags = OGGZ_FLUSH_AFTER;
    int rc = oggz_write_feed(oggz, &op, streamnum, flags, NULL);
    std::cout << "write bos" << streamnum << " " << op.packetno
              << " @ " << op.granulepos
              << " " << std::hex << flags << std::dec
              << " rc=" << rc
              << "\n";
    assert(rc == 0);
    while (oggz_write (oggz, 32) > 0);
    return stream_size;
}

static void
oggpb_write_footail(OGGZ* oggz, int streamnum, int pktnum, uint64_t gran)
{
    ogg_packet op;
    memset(&op, 0, sizeof(ogg_packet));

    op.granulepos = gran;
    op.packetno = pktnum;
    op.e_o_s = 1;
    const int flags = OGGZ_FLUSH_AFTER;
    std::cout << "write eos" << streamnum << " " << op.packetno
              << " @ " << op.granulepos
              << " " << std::hex << flags << std::dec
              << "\n";
    int rc = oggz_write_feed(oggz, &op, streamnum, flags, NULL);
    assert(rc == 0);
    while (oggz_write (oggz, 32) > 0);
}

static size_t
oggpb_write_foobone(OGGZ* oggz, int streamnum, int pktnum, uint64_t gran,
                    const std::string& name, int nnums)
{
    size_t stream_size = 0;
    ogg_packet op;
    memset(&op, 0, sizeof(ogg_packet));

    foo::Foo f;
    f.set_text(name);
    for (int n=0; n<nnums; ++n) {
        f.add_numbers(n);
    }

    size_t siz = f.ByteSize();
    stream_size += siz;
    unsigned char* buf = (unsigned char*)malloc(siz);
    assert(buf);
    f.SerializeToArray(buf,siz);
    op.packet = buf;
    op.bytes = siz;
    op.granulepos = gran;
    op.packetno = pktnum;
            
    int flags = OGGZ_FLUSH_AFTER;
    std::cout << "write stream" << streamnum << " " << op.packetno
              << " @ " << op.granulepos
              << " " << std::hex << flags << std::dec
              << " " << siz << " B"
              << "\n";
    int rc = oggz_write_feed(oggz, &op, streamnum, flags, NULL);
    if (rc != 0) {
        std::cout << "write failed: " << rc << "\n";
    }
    while (oggz_write (oggz, 32) > 0);
    return stream_size;
}

static int
oggpb_write(const char* filename)
{
    OGGZ* oggz = oggz_open (filename, OGGZ_WRITE);

    // simulate writing a few streams with different time granule
    // series.

    const int nloops = 10;
    const int nstreams = 3;
    std::vector<std::string> stream_names{"foo","bar","baz"};
    const std::vector<int> istreams{2,3,4};


    std::vector<size_t> pkt_count{0,0,0};
    std::vector<size_t> stream_size{0,0,0};
    size_t total_size = 0;


    const int fistream = 1;
    oggpb_write_fishead(oggz, fistream);

    for (int istream=0; istream<nstreams; ++istream) {
        const int plstream = fistream + istream + 1;
        size_t siz = oggpb_write_foohead(oggz, plstream);
        stream_size[istream] += siz;
        total_size += 1;
        pkt_count[istream]++;
    }


    for (uint32_t istream=0; istream<nstreams; ++istream) {
        const int pktnum = istream+1;
        size_t siz = oggpb_write_fisbone(oggz, fistream, pktnum,
                                         istreams[istream],
                                         stream_names[istream]);
        total_size += siz;
    }

    oggpb_write_fistail(oggz, fistream, nstreams+2);

    // ogg skeleton 4 could have index pages, but we are using 3.

    std::vector<int64_t> tnow{10,100,1000}, dt{1000,100,10};
    std::vector<size_t> nnumbers{1000,100,10};

    for (int loop=0; loop<nloops; ++loop) {

        for (int istream=0; istream<nstreams; ++istream) {

            const int plstream = fistream + istream + 1;

            const int npkts = 2 + 1<<(istream+1);
            for (int ipkt=0; ipkt<npkts; ++ipkt) {
                uint64_t gran = tnow[istream] + (pkt_count[istream]) * dt[istream];
                int pktnum = ++pkt_count[istream];

                oggpb_write_foobone(oggz, plstream, pktnum, gran,
                                    stream_names[istream], 
                                    nnumbers[istream]);

            } // packet
        } // stream
    
    } // loop

    // EOS on foo streams
    for (int istream=0; istream<nstreams; ++istream) {
        const int plstream = fistream + istream + 1;
        uint64_t gran = tnow[istream] + (pkt_count[istream]) * dt[istream];
        int pktnum = ++pkt_count[istream];
        oggpb_write_footail(oggz, plstream, pktnum, gran);
    }
    

    // final write
    while (oggz_write (oggz, 32) > 0);

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
