import { NavLink, Outlet } from "react-router-dom";
import {
  BookOpen,
  Compass,
  Users,
  Library,
  MessageSquare,
  Menu,
  X,
} from "lucide-react";
import { useState } from "react";

const navItems = [
  { to: "/", icon: BookOpen, label: "Feed" },
  { to: "/explore", icon: Compass, label: "Explore Clubs" },
  { to: "/my-clubs", icon: Users, label: "My Clubs" },
  { to: "/library", icon: Library, label: "Library" },
  { to: "/forum", icon: MessageSquare, label: "Forum" },
];

export default function Layout() {
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <div className="min-h-screen flex flex-col">
      <header className="bg-warm-50/80 backdrop-blur-md border-b border-warm-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex items-center justify-between h-16">
          <NavLink to="/" className="flex items-center gap-2 group">
            <BookOpen className="w-6 h-6 text-sage-600 group-hover:text-sage-500 transition-colors" />
            <span className="font-serif text-xl font-semibold text-warm-900 tracking-tight">
              Bookish
            </span>
          </NavLink>

          <nav className="hidden md:flex items-center gap-1">
            {navItems.map(({ to, icon: Icon, label }) => (
              <NavLink
                key={to}
                to={to}
                end={to === "/"}
                className={({ isActive }) =>
                  `flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                    isActive
                      ? "bg-sage-100 text-sage-700"
                      : "text-warm-600 hover:text-warm-800 hover:bg-warm-100"
                  }`
                }
              >
                <Icon className="w-4 h-4" />
                {label}
              </NavLink>
            ))}
          </nav>

          <button
            onClick={() => setMobileOpen(!mobileOpen)}
            className="md:hidden p-2 rounded-lg hover:bg-warm-100 transition-colors"
          >
            {mobileOpen ? (
              <X className="w-5 h-5 text-warm-700" />
            ) : (
              <Menu className="w-5 h-5 text-warm-700" />
            )}
          </button>
        </div>

        {mobileOpen && (
          <nav className="md:hidden border-t border-warm-200 bg-warm-50 px-4 pb-4 pt-2">
            {navItems.map(({ to, icon: Icon, label }) => (
              <NavLink
                key={to}
                to={to}
                end={to === "/"}
                onClick={() => setMobileOpen(false)}
                className={({ isActive }) =>
                  `flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-all ${
                    isActive
                      ? "bg-sage-100 text-sage-700"
                      : "text-warm-600 hover:text-warm-800 hover:bg-warm-100"
                  }`
                }
              >
                <Icon className="w-4 h-4" />
                {label}
              </NavLink>
            ))}
          </nav>
        )}
      </header>

      <main className="flex-1">
        <Outlet />
      </main>

      <footer className="border-t border-warm-200 bg-warm-50 py-8 mt-12">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          <p className="text-sm text-warm-500">
            Bookish &mdash; Find your reading community in Seattle
          </p>
        </div>
      </footer>
    </div>
  );
}
