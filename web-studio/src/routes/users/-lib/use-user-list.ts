import { useState } from 'react'
import type { AdminUser } from '#/lib/admin'

export function useUserList(users: AdminUser[], scope: string) {
  const [state, setState] = useState({
    scope,
    search: '',
    page: 1,
    pageSize: 20,
  })
  const current =
    state.scope === scope
      ? state
      : { scope, search: '', page: 1, pageSize: state.pageSize }

  if (state.scope !== scope) setState(current)

  const query = current.search.trim().toLowerCase()
  const filtered = users.filter((user) =>
    user.userId.toLowerCase().includes(query),
  )
  const pageCount = Math.max(1, Math.ceil(filtered.length / current.pageSize))
  const page = Math.min(current.page, pageCount)

  return {
    search: current.search,
    page,
    pageSize: current.pageSize,
    pageCount,
    total: filtered.length,
    users: filtered.slice(
      (page - 1) * current.pageSize,
      page * current.pageSize,
    ),
    setSearch: (search: string) => setState({ ...current, search, page: 1 }),
    setPage: (next: number) =>
      setState({ ...current, page: Math.max(1, Math.min(next, pageCount)) }),
    setPageSize: (pageSize: number) =>
      setState({ ...current, pageSize, page: 1 }),
  }
}
