#include "raftctl/raftctl_command.h"

#include <iostream>

int main(int argc, char **argv)
{
    return cpr::tools::raftctl::RunRaftCtl(argc, argv, std::cout, std::cerr);
}
