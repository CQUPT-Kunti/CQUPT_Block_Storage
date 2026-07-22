#pragma once

#include <iosfwd>

namespace cpr::tools::client
{

    int RunClientTool(int argc,
                      const char *const argv[],
                      std::ostream &out,
                      std::ostream &err);

} // namespace cpr::tools::client
