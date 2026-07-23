#include <chrono>
#include <iostream>
#include <thread>

#include "config/config_loader.h"
#include "server/server.h"

namespace
{

    void PrintHelp(std::ostream &out)
    {
        out << "Usage: cpr_server --config PATH\n";
    }

} // namespace

int main(int argc, char **argv)
{
    if (argc != 3 || std::string(argv[1]) == "--help")
    {
        PrintHelp(std::cout);
        return argc == 2 ? 0 : 2;
    }
    if (std::string(argv[1]) != "--config")
    {
        PrintHelp(std::cerr);
        return 2;
    }

    cpr::config::Config config;
    cpr::common::Status status =
        cpr::config::ConfigLoader::LoadFromFile(argv[2], &config);
    if (!status.ok())
    {
        std::cerr << status.ToString() << '\n';
        return 1;
    }

    cpr::server::ServerApplication server;
    status = server.Initialize(config);
    if (!status.ok())
    {
        std::cerr << status.ToString() << '\n';
        return 1;
    }
    status = server.Start();
    if (!status.ok())
    {
        std::cerr << status.ToString() << '\n';
        return 1;
    }

    while (!server.HasShutdownRequest())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    status = server.Stop();
    if (!status.ok())
    {
        std::cerr << status.ToString() << '\n';
        return 1;
    }
    return 0;
}
