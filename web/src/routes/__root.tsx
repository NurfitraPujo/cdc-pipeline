import { TanStackDevtools } from "@tanstack/react-devtools";
import type { QueryClient } from "@tanstack/react-query";
import {
	createRootRouteWithContext,
	HeadContent,
	Navigate,
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
		const { isAuthenticated } = useAuthStore.getState();
		const isLoginPage = location.pathname === "/login";

		if (!isAuthenticated && !isLoginPage) {
			throw Navigate({ to: "/login" });
		}

		if (isAuthenticated && isLoginPage) {
			throw Navigate({ to: "/dashboard" });
		}
	},
	shellComponent: RootDocument,
});

function RootDocument({ children }: { children: React.ReactNode }) {
	return (
		<html lang="en" suppressHydrationWarning>
			<head>
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
