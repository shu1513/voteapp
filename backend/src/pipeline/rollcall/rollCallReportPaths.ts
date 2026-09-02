import { homedir } from "node:os";
import { isAbsolute, relative, resolve, sep } from "node:path";

// The roll-call scripts record the input paths they ran with in committed
// report JSON. Those files are read by reviewers on other machines, so a
// developer-local absolute path (`/Users/<name>/...`) is noise there and
// names nothing reproducible. Report the path the way an operator would
// retype it: relative to the working directory when it lives under it,
// `~/...` when it lives under the home directory, and untouched otherwise.
export function reportPath(path: string, cwd: string = process.cwd(), home: string = homedir()): string {
  const absolute = resolve(cwd, path);
  const fromCwd = relative(cwd, absolute);
  if (fromCwd === "") {
    return ".";
  }
  if (!fromCwd.startsWith("..") && !isAbsolute(fromCwd)) {
    return fromCwd;
  }
  const fromHome = relative(home, absolute);
  if (fromHome !== "" && !fromHome.startsWith("..") && !isAbsolute(fromHome)) {
    return `~${sep}${fromHome}`;
  }
  return absolute;
}
