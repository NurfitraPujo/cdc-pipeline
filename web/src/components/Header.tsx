import { Link } from "@tanstack/react-router";
import { LogOut } from "lucide-react";
import { useAuthStore } from "@/stores/authStore";
import ThemeToggle from "./ThemeToggle";

interface HeaderProps {
	showNav?: boolean;
}

export function Header({ showNav = true }: HeaderProps) {
	const { isAuthenticated, logout } = useAuthStore();

	return (
		<header className="sticky top-0 z-50 border-b border-[var(--line)] bg-[var(--header-bg)] px-4 backdrop-blur-lg">
			<nav className="page-wrap flex flex-wrap items-center gap-x-3 gap-y-2 py-3 sm:py-4">
				<h2 className="m-0 flex-shrink-0 text-base font-semibold tracking-tight">
					<Link
						to="/"
						className="inline-flex items-center gap-2 rounded-full border border-[var(--chip-line)] bg-[var(--chip-bg)] px-3 py-1.5 text-sm text-[var(--sea-ink)] no-underline shadow-[0_8px_24px_rgba(30,90,72,0.08)] sm:px-4 sm:py-2"
					>
						<span className="h-2 w-2 rounded-full bg-[linear-gradient(90deg,#56c6be,#7ed3bf)]" />
						CDC Pipeline
					</Link>
				</h2>

				<div className="ml-auto flex items-center gap-1.5 sm:gap-2">
					<ThemeToggle />

					{isAuthenticated && (
						<button
							type="button"
							onClick={logout}
							className="flex items-center gap-1.5 rounded-xl px-3 py-2 text-sm font-medium text-[var(--sea-ink-soft)] transition hover:bg-[var(--link-bg-hover)] hover:text-[var(--sea-ink)]"
							title="Logout"
						>
							<LogOut className="h-4 w-4" />
							<span className="hidden sm:inline">Logout</span>
						</button>
					)}
				</div>

				{showNav && (
					<div className="order-3 flex w-full flex-wrap items-center gap-x-4 gap-y-1 pb-1 text-sm font-semibold sm:order-2 sm:w-auto sm:flex-nowrap sm:pb-0">
						<Link
							to="/"
							className="nav-link"
							activeProps={{ className: "nav-link is-active" }}
						>
							Home
						</Link>
						<Link
							to="/about"
							className="nav-link"
							activeProps={{ className: "nav-link is-active" }}
						>
							About
						</Link>
						<a
							href="https://tanstack.com/start/latest/docs/framework/react/overview"
							className="nav-link"
							target="_blank"
							rel="noreferrer"
						>
							Docs
						</a>
						<details className="relative w-full sm:w-auto">
							<summary className="nav-link list-none cursor-pointer">
								Demos
							</summary>
							<div className="mt-2 min-w-56 rounded-xl border border-[var(--line)] bg-[var(--header-bg)] p-2 shadow-lg sm:absolute sm:right-0">
								<a
									href="/demo/tanstack-query"
									className="block rounded-lg px-3 py-2 text-sm text-[var(--sea-ink-soft)] no-underline transition hover:bg-[var(--link-bg-hover)] hover:text-[var(--sea-ink)]"
								>
									TanStack Query
								</a>
								<a
									href="/demo/table"
									className="block rounded-lg px-3 py-2 text-sm text-[var(--sea-ink-soft)] no-underline transition hover:bg-[var(--link-bg-hover)] hover:text-[var(--sea-ink)]"
								>
									TanStack Table
								</a>
								<a
									href="/demo/store"
									className="block rounded-lg px-3 py-2 text-sm text-[var(--sea-ink-soft)] no-underline transition hover:bg-[var(--link-bg-hover)] hover:text-[var(--sea-ink)]"
								>
									Store
								</a>
							</div>
						</details>
					</div>
				)}
			</nav>
		</header>
	);
}
