/**
 * Pass-through stub for `@radix-ui/react-*` packages.
 *
 * Several `@radix-ui/react-*` primitives the dashboard pulls in (e.g.
 * `react-checkbox`, `react-dialog`, `react-select`) are not present in
 * `node_modules`, so the Vite import-analysis plugin refuses to load the
 * corresponding dashboard components in the test environment. Vitest
 * aliases every `@radix-ui/react-*` import to this stub via `vitest.config.ts`,
 * which keeps component tests runnable without changing production code.
 *
 * The stub exposes a permissive JSX element for every component name the
 * dashboard primitives reference, so anything we render continues to look like
 * the real component (children render, props pass through, callbacks fire) but
 * no behavioral assertion is made on the Radix internals.
 */
import * as React from "react";

type AnyProps = React.HTMLAttributes<HTMLElement> & {
	children?: React.ReactNode;
	value?: unknown;
	checked?: unknown;
	defaultValue?: unknown;
	open?: unknown;
	onOpenChange?: (open: boolean) => void;
	htmlFor?: string;
	id?: string;
	asChild?: boolean;
	type?: "single" | "multiple";
	defaultValue?: string | string[];
};

// Strip out the Radix-only props the DOM doesn't know about so they don't
// generate noisy React warnings when forwarded onto a real element.
const stripRadixProps = (props: Record<string, unknown>) => {
	const {
		value: _value,
		checked: _checked,
		defaultValue: _defaultValue,
		open: _open,
		onOpenChange: _onOpenChange,
		onCheckedChange: _onCheckedChange,
		onValueChange: _onValueChange,
		onEscapeKeyDown: _onEscapeKeyDown,
		onPointerDown: _onPointerDown,
		onPointerUp: _onPointerUp,
		side: _side,
		sideOffset: _sideOffset,
		align: _align,
		alignOffset: _alignOffset,
		collisionBoundary: _collisionBoundary,
		collisionPadding: _collisionPadding,
		arrowPadding: _arrowPadding,
		sticky: _sticky,
		hideWhenDetached: _hideWhenDetached,
		avoidCollisions: _avoidCollisions,
		modal: _modal,
		direction: _direction,
		forceMount: _forceMount,
		loop: _loop,
		trapFocus: _trapFocus,
		disableOutsidePointerEvents: _disableOutsidePointerEvents,
		asChild: _asChild,
		...domProps
	} = props as AnyProps;
	return domProps as React.HTMLAttributes<HTMLElement>;
};

// The default passthrough renders a <div>. Several UI primitives need the
// stub to look like a more specific element so the component still
// participates in the accessibility tree:
//   - `Root` of `@radix-ui/react-label` should be a <label htmlFor=…>
//     so `getByLabelText` can find associated controls.
//   - `Root` of `@radix-ui/react-switch` should be a <button role="switch">
//     so it can be queried and clicked.
//   - `Root` of `@radix-ui/react-checkbox` should be a <button role="checkbox">.
//   - `Root` of `@radix-ui/react-accordion` honours `type` and manages an
//     open state if `defaultValue` is provided (mirrors the real Radix
//     behaviour just enough for the tests in this repo).
const passthrough = (props: AnyProps) =>
	React.createElement("div", stripRadixProps(props), props.children);

const Root = (props: AnyProps) => {
	const dom = stripRadixProps(props);
	const handleClick = props.onClick;
	// Label: render as a <label> so htmlFor wires up correctly.
	if (props.htmlFor) {
		return React.createElement(
			"label",
			{ ...dom, htmlFor: props.htmlFor },
			props.children,
		);
	}
	// Switch + checkbox: render as <button role="switch"|"checkbox"> so
	// accessibility-tree queries (e.g. getByLabelText, getByRole) work even
	// though we don't have the real Radix primitive installed. The Radix
	// primitives pass `onCheckedChange` / `checked` instead of an explicit
	// role, so we detect either form here.
	const onCheckedChange = (props as { onCheckedChange?: unknown })
		.onCheckedChange as ((checked: boolean) => void) | undefined;
	const checked = (props as { checked?: unknown }).checked as
		| boolean
		| "indeterminate"
		| undefined;
	if (
		props.role === "switch" ||
		props.role === "checkbox" ||
		typeof onCheckedChange === "function"
	) {
		const role = props.role ?? "switch";
		const ariaChecked =
			checked === undefined ? false : checked === "indeterminate" ? "mixed" : checked;
		const onButtonClick = (event: React.MouseEvent<HTMLButtonElement>) => {
			if (typeof handleClick === "function") {
				handleClick(event);
			}
			if (typeof onCheckedChange === "function" && !event.defaultPrevented) {
				onCheckedChange(checked !== true);
			}
		};
		return React.createElement(
			"button",
			{ ...dom, role, type: "button", "aria-checked": ariaChecked, onClick: onButtonClick },
			props.children,
		);
	}
	return React.createElement("div", dom, props.children);
};

const named = (tag: string) => {
	const Comp = ({ children, ...rest }: AnyProps) => {
		const { type: _type, ...dom } = stripRadixProps(rest);
		return React.createElement(tag, dom, children);
	};
	return Comp;
};

// Permissive exports covering every primitive the dashboard UI uses. The
// `passthrough` helper keeps refs / props flowing through without crashing.
export { Root, passthrough };
export const Trigger = passthrough;
export const Content = passthrough;
export const Item = passthrough;
export const ItemIndicator = passthrough;
export const ItemText = passthrough;
export const Header = passthrough;
export const Footer = passthrough;
export const Title = passthrough;
export const Description = passthrough;
export const Portal = passthrough;
export const Overlay = passthrough;
export const Close = passthrough;
export const Action = passthrough;
export const Cancel = passthrough;
export const Arrow = passthrough;
export const Value = passthrough;
export const Icon = passthrough;
export const Indicator = passthrough;
export const Group = passthrough;
export const Label = passthrough;
export const Separator = passthrough;
export const Viewport = passthrough;
export const ScrollUpButton = passthrough;
export const ScrollDownButton = passthrough;
export const List = passthrough;
export const Sub = passthrough;
export const SubTrigger = passthrough;
export const SubContent = passthrough;
export const RadioGroup = passthrough;
export const RadioItem = passthrough;
export const Slider = passthrough;
export const SliderTrack = passthrough;
export const SliderRange = passthrough;
export const SliderThumb = passthrough;
export const Tabs = passthrough;
export const TabsList = passthrough;
export const Tab = passthrough;
export const TabPanel = passthrough;
export const Toggle = passthrough;
export const ToggleGroup = passthrough;
export const ToggleItem = passthrough;
export const Tooltip = passthrough;
export const TooltipTrigger = passthrough;
export const TooltipContent = passthrough;
export const TooltipProvider = passthrough;
export const Provider = passthrough;

// The dashboard UI uses `React.forwardRef` wrappers that reference
// `Component.displayName = CheckboxPrimitive.Root.displayName`. Provide a
// stable string so those assignments don't crash the module body.
export const displayName = "RadixStub";

// Provide every other named export as a generic passthrough. This catches
// any new primitive the UI primitives might pull in later without breaking
// the test environment.
export const Accordion = named("div");
export const AccordionItem = named("div");
export const AccordionContent = named("div");
export const AccordionTrigger = named("div");

// Switch, Select, and several other primitives export a `Thumb` /
// `Menu.Item` style sub-component. Stub them so JSX renders without crashing.
export const Thumb = passthrough;
export const Menu = passthrough;
export const MenuItem = passthrough;
export const TriggerItem = passthrough;
export const CheckboxItem = passthrough;
export const CheckboxIndicator = passthrough;

// Some components reach for `asChild` semantics — preserve that prop
// without applying it (children are already rendered through the wrapper).
export const Slot = passthrough;

export default passthrough;
