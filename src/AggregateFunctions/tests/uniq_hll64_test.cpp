#include <IO/ReadBufferFromMemory.h>
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <fstream>

int main(int, char **)
{
    DB::HyperLogLogWrapper<String, 12> hll;

    std::ifstream in("/root/ClickHouse/src/AggregateFunctions/tests/out.txt", std::ios_base::binary);
    std::cout << "we are parsing hll..." << std::endl;
    if (in)
    {
        std::cout << "file has opened..." << std::endl;
        //get length of file
        in.seekg(0, in.end);
        size_t length = in.tellg();
        in.seekg(0, in.beg);
        char * buffer = new char[length];
        in.read(buffer, length);
        std::cout << "format is " << static_cast<int>(buffer[0]) << std::endl;
        std::cout << "precision is " << static_cast<int>(buffer[1]) << std::endl;
        in.close();

        DB::ReadBufferFromMemory content(buffer, length);

        hll.insert(content);
        std::cout << hll.size() << std::endl;
    }
	
}
