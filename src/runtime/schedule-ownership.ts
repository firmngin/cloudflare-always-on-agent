/** A successful sync owns scheduling even when the desired list is empty. */
export function scheduleSyncOwnsAgent(desired: readonly unknown[] | null): boolean {
  return desired !== null
}
