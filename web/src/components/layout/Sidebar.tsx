import { Link, useLocation } from "@tanstack/react-router";
import {
	Database,
	GitBranch,
	HardDrive,
	LayoutDashboard,
	Settings,
} from "lucide-react";
import { useAuthStore } from "@/stores/authStore";

interface NavItem {
	to: string;
	label: string;
	icon: React.ComponentType<{ className?: string }>;
}

const navItems: NavItem[] = [
	{ to: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
	{ to: "/pipelines", label: "Pipelines", icon: GitBranch },
	{ to: "/sources", label: "Sources", icon: Database },
	{ to: "/sinks", label: "Sinks", icon: HardDrive },
	{ to: "/config", label: "Configuration", icon: Settings },
];

export function Sidebar() {
	const location = useLocation();
	const { isAuthenticated } = useAuthStore();

	if (!isAuthenticated) {
		return null;
	}

	return (
		<aside className="fixed left-0 top-0 z-40 h-screen w-64 border-r border-[var(--line)] bg-[var(--header-bg)]">
			<div className="flex h-full flex-col">
				{/* Logo */}
				<div className="flex h-16 items-center border-b border-[var(--line)] px-6">
					<Link
						to="/"
						className="flex items-center gap-2 text-lg font-semibold text-[var(--sea-ink)] no-underline"
					>
						<span className="h-3 w-3 rounded-full bg-[linear-gradient(90deg,#56c6be,#7ed3bf)]" />
						CDC Pipeline
					</Link>
				</div>

				{/* Navigation */}
				<nav className="flex-1 space-y-1 px-3 py-4">
					{navItems.map((item) => {
						const isActive = location.pathname.startsWith(item.to);
						const Icon = item.icon;

						return (
							<Link
								key={item.to}
								to={item.to}
								className={`flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors ${
									isActive
										? "bg-[var(--link-bg-hover)] text-[var(--sea-ink)]"
										: "text-[var(--sea-ink-soft)] hover:bg-[var(--link-bg-hover)] hover:text-[var(--sea-ink)]"
								}`}
							>
								<Icon className="h-5 w-5" />
								{item.label}
							</Link>
						);
					})}
				</nav>

				{/* Footer */}
				<div className="border-t border-[var(--line)] p-4">
					<p className="text-xs text-[var(--sea-ink-soft)]">
						CDC Pipeline Dashboard
					</p>
				</div>
			</div>
		</aside>
	);
}

export default Sidebar;
