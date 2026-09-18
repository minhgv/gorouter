// copyText writes to the clipboard with a legacy selection fallback for plain
// HTTP origins and restrictive browser policies that expose the Clipboard API
// while rejecting writes. Returns true when the value reached the clipboard.
export async function copyText(value: string): Promise<boolean> {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(value)
      return true
    } catch {
      // Fall through to the legacy selection path.
    }
  }
  const input = document.createElement('textarea')
  input.value = value
  input.setAttribute('readonly', '')
  input.style.position = 'fixed'
  input.style.opacity = '0'
  document.body.appendChild(input)
  input.select()
  try {
    return document.execCommand('copy')
  } finally {
    input.remove()
  }
}
