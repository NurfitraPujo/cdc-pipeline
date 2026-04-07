import { useCallback, useEffect, useState } from "react";

export function useLocalStorage<T>(
	key: string,
	initialValue: T,
): [T | undefined, (value: T | ((prev: T) => T)) => void, () => void] {
	// Initialize with undefined to prevent SSR/hydration mismatch flicker
	const [storedValue, setStoredValue] = useState<T | undefined>(undefined);
	const [, setIsInitialized] = useState(false);

	useEffect(() => {
		try {
			const item = window.localStorage.getItem(key);
			if (item) {
				setStoredValue(JSON.parse(item));
			} else {
				setStoredValue(initialValue);
			}
		} catch (error) {
			console.warn(`Error reading localStorage key "${key}":`, error);
			setStoredValue(initialValue);
		}
		setIsInitialized(true);
	}, [key, initialValue]);

	const setValue = useCallback(
		(value: T | ((prev: T) => T)) => {
			try {
				setStoredValue((prev) => {
					const valueToStore = value instanceof Function ? value(prev ?? initialValue) : value;
					window.localStorage.setItem(key, JSON.stringify(valueToStore));
					return valueToStore;
				});
			} catch (error) {
				console.warn(`Error setting localStorage key "${key}":`, error);
			}
		},
		[key, initialValue],
	);

	const removeValue = useCallback(() => {
		try {
			window.localStorage.removeItem(key);
			setStoredValue(initialValue);
		} catch (error) {
			console.warn(`Error removing localStorage key "${key}":`, error);
		}
	}, [key, initialValue]);

	// Return undefined during SSR/hydration to prevent flicker
	// Component should handle undefined state with loading/fallback UI
	return [storedValue, setValue, removeValue];
}
