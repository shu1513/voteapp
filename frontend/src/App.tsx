import { Link, Outlet } from "react-router-dom";

export function App() {
  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <header className="border-b border-gray-200 bg-white">
        <div className="mx-auto flex max-w-3xl items-center justify-between px-4 py-3">
          <Link to="/" className="text-lg font-bold text-blue-800">
            VoteApp
          </Link>
          <nav className="text-sm">
            <Link to="/disclaimer" className="text-gray-600 hover:underline">
              Disclaimer
            </Link>
          </nav>
        </div>
      </header>
      <main>
        <Outlet />
      </main>
      <footer className="mt-12 border-t border-gray-200 py-6 text-center text-xs text-gray-500">
        <p>
          Independent, nonpartisan, AI-assisted election research. Not an official election source —{" "}
          <Link to="/disclaimer" className="underline">
            read the Disclaimer
          </Link>
          .
        </p>
      </footer>
    </div>
  );
}
