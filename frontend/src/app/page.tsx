import { Workspace } from "@/components/workspace";
import snapshot from "@/lib/project-snapshot.json";
import type { Overview } from "@/lib/types";

export default function Home() {
  return <Workspace initialData={snapshot as Overview} />;
}
