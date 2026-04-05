import { Settings } from "lucide-react";
import { lazy } from "react";

const Admin = lazy(() => import("../../pages/Admin").then((m) => ({ default: m.Admin })));

export const adminRoute = {
  path: "admin",
  element: <Admin />,
};

export const adminNavItem = {
  path: "/app/admin",
  icon: Settings,
  labelKey: "nav.admin",
};
