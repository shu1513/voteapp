import { useEffect } from "react";
import { useNavigate } from "react-router";
import { LoadingNotice } from "../components/Status";

// Retired: followed candidates moved into the My Picks page. The route stays
// as a redirect so old bookmarks and links in already-sent notification
// emails keep working. Client-side navigate (the house redirect pattern —
// see HomePage/WelcomePage), replace so Back doesn't bounce.

export function FollowsPage() {
  const navigate = useNavigate();
  useEffect(() => {
    void navigate("/me/picks", { replace: true });
  }, [navigate]);
  return <LoadingNotice text="Loading…" />;
}

export default FollowsPage;
