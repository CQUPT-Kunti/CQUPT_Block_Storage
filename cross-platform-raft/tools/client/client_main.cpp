#include "client/client_command.h"

#include <iostream>

int main(int argc, char **argv)
{
    return cpr::tools::client::RunClientTool(argc, argv, std::cout, std::cerr);
}
