#pragma once

#include <iosfwd>

namespace cpr::tools::wal_dump
{

    int RunWalDump(int argc,
                   const char *const argv[],
                   std::ostream &out,
                   std::ostream &err);

} // namespace cpr::tools::wal_dump
