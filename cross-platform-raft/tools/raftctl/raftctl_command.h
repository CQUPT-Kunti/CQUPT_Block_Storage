#pragma once

#include <iosfwd>

namespace cpr::tools::raftctl
{

    int RunRaftCtl(int argc,
                   const char *const argv[],
                   std::ostream &out,
                   std::ostream &err);

} // namespace cpr::tools::raftctl
