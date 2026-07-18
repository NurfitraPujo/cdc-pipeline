import { TanStackDevtools } from "@tanstack/react-devtools";
import type { QueryClient } from "@tanstack/react-query";
import {
	createRootRouteWithContext,
	HeadContent,
	redirect,
	Scripts,
	useLocation,
} from "@tanstack/react-router";
import { TanStackRouterDevtoolsPanel } from "@tanstack/react-router-devtools";
import Footer from "../components/Footer";
import { Header } from "../components/Header";
import Sidebar from "../components/layout/Sidebar";

import TanStackQueryDevtools from "../integrations/tanstack-query/devtools";
import TanStackQueryProvider from "../integrations/tanstack-query/root-provider";
import StoreDevtools from "../lib/demo-store-devtools";
import { useAuthStore } from "../stores/authStore";
import appCss from "../styles.css?url";

interface MyRouterContext {
	queryClient: QueryClient;
}

const THEME_INIT_SCRIPT = `(function(){try{var stored=window.localStorage.getItem('theme');var mode=(stored==='light'||stored==='dark'||stored==='auto')?stored:'auto';var prefersDark=window.matchMedia('(prefers-color-scheme: dark)').matches;var resolved=mode==='auto'?(prefersDark?'dark':'light'):mode;var root=document.documentElement;root.classList.remove('light','dark');root.classList.add(resolved);if(mode==='auto'){root.removeAttribute('data-theme')}else{root.setAttribute('data-theme',mode)}root.style.colorScheme=resolved;}catch(e){}})();`;

function readAuthFromStorage(): {
	isAuthenticated: boolean;
	hasToken: boolean;
} {
	if (typeof window === "undefined") {
		return { isAuthenticated: false, hasToken: false };
	}
	const raw = window.localStorage.getItem("cdc-auth-storage");
	if (!raw) return { isAuthenticated: false, hasToken: false };
	try {
		const parsed = JSON.parse(raw) as {
			state?: { isAuthenticated?: boolean; token?: string | null };
		};
		const state = parsed?.state;
		const isAuthenticated = state?.isAuthenticated === true;
		const hasToken = typeof state?.token === "string" && state.token.length > 0;
		return { isAuthenticated, hasToken };
	} catch {
		return { isAuthenticated: false, hasToken: false };
	}
}

export const Route = createRootRouteWithContext<MyRouterContext>()({
	head: () => ({
		meta: [
			{
				charSet: "utf-8",
			},
			{
				name: "viewport",
				content: "width=device-width, initial-scale=1",
			},
			{
				title: "CDC Pipeline Dashboard",
			},
		],
		links: [
			{
				rel: "stylesheet",
				href: appCss,
			},
		],
	}),
	beforeLoad: ({ location }) => {
		// Auth check runs only on the client. During SSR, localStorage is
		// unavailable, so we let the route render and the client-side hydration
		// will re-evaluate and redirect if needed.
		if (typeof window === "undefined") {
			return;
		}

		// Belt-and-braces check: trust the persisted Zustand state, but also
		// require a non-empty token. A stale `isAuthenticated: true` without a
		// token (e.g. localStorage tampered or migration glitch) should not
		// grant access — that mismatch was the root cause of the 401 bounce
		// loop fixed in T0-7.
		const { isAuthenticated, hasToken } = readAuthFromStorage();
		const hasValidSession = isAuthenticated && hasToken;
		const isLoginPage = location.pathname === "/login";

		if (!hasValidSession && !isLoginPage) {
			throw redirect({ to: "/login" });
		}

		if (hasValidSession && isLoginPage) {
			throw redirect({ to: "/dashboard" });
		}
	},
	shellComponent: RootDocument,
});

function RootDocument({ children }: { children: React.ReactNode }) {
	return (
		<html lang="en" suppressHydrationWarning>
			<head>
				{/* biome-ignore lint/security/noDangerouslySetInnerHtml: Required for theme initialization before React hydrates */}
				<script dangerouslySetInnerHTML={{ __html: THEME_INIT_SCRIPT }} />
				<HeadContent />
			</head>
			<body className="font-sans antialiased [overflow-wrap:anywhere] selection:bg-[rgba(79,184,178,0.24)]">
				<TanStackQueryProvider>
					<RootLayout>{children}</RootLayout>
					<TanStackDevtools
						config={{
							position: "bottom-right",
						}}
						plugins={[
							{
								name: "Tanstack Router",
								render: <TanStackRouterDevtoolsPanel />,
							},
							TanStackQueryDevtools,
							StoreDevtools,
						]}
					/>
				</TanStackQueryProvider>
				<Scripts />
			</body>
		</html>
	);
}

function RootLayout({ children }: { children: React.ReactNode }) {
	const location = useLocation();
	const isLoginPage = location.pathname === "/login";
	const { isAuthenticated } = useAuthStore();
	const showSidebar = !isLoginPage && isAuthenticated;

	return (
		<>
			{!isLoginPage && <Header showNav={!showSidebar} />}
			{showSidebar && <Sidebar />}
			<main className={showSidebar ? "pl-64" : ""}>{children}</main>
			{!isLoginPage && <Footer />}
		</>
	);
}
