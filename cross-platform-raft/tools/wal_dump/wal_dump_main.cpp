#include "wal_dump/wal_dump_command.h"

#include <iostream>

int main(int argc, char **argv)
{
    return cpr::tools::wal_dump::RunWalDump(argc, argv, std::cout, std::cerr);
}
