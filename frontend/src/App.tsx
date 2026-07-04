import { Link, Outlet } from "react-router-dom";

export function App() {
  return (
    <div className="min-h-screen bg-white text-ink">
      <header className="border-b border-line bg-white">
        <div className="mx-auto flex max-w-3xl items-center justify-between px-4 py-4">
          <Link to="/" className="text-xl font-extrabold tracking-tight text-rausch">
            VoteApp
          </Link>
          <nav className="text-sm">
            <Link to="/disclaimer" className="text-ink-soft hover:text-ink">
              Disclaimer
            </Link>
          </nav>
        </div>
      </header>
      <main>
        <Outlet />
      </main>
      <footer className="mt-16 border-t border-line py-8 text-center text-xs text-ink-soft">
        <p>
          Independent, nonpartisan, AI-assisted election research. Not an official election source —{" "}
          <Link to="/disclaimer" className="underline hover:text-ink">
            read the Disclaimer
          </Link>
          .
        </p>
      </footer>
    </div>
  );
}
