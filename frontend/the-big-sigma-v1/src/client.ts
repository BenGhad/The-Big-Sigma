const DEFAULT_API_BASE_URL = 'http://127.0.0.1:8000'

const configuredBaseUrl = import.meta.env.VITE_API_BASE_URL as string | undefined
const API_BASE_URL = (configuredBaseUrl ?? DEFAULT_API_BASE_URL).replace(/\/+$/, '')

type ErrorEnvelope = {
  error?: {
    code?: string
    message?: string
    details?: unknown
  }
}

export class ApiError extends Error {
  readonly status: number
  readonly code?: string
  readonly details?: unknown

  constructor(message: string, status: number, code?: string, details?: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
    this.details = details
  }
}

function toAbsoluteUrl(path: string): string {
  if (/^https?:\/\//.test(path)) {
    return path
  }
  const normalizedPath = path.startsWith('/') ? path : `/${path}`
  return `${API_BASE_URL}${normalizedPath}`
}

async function parseJson<T>(response: Response): Promise<T | null> {
  const text = await response.text()
  if (!text) {
    return null
  }
  return JSON.parse(text) as T
}

export async function apiRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(toAbsoluteUrl(path), init)
  const payload = await parseJson<ErrorEnvelope | T>(response)

  if (!response.ok) {
    const error = payload as ErrorEnvelope | null
    const message = error?.error?.message ?? response.statusText ?? 'Request failed'
    throw new ApiError(message, response.status, error?.error?.code, error?.error?.details)
  }

  return payload as T
}
