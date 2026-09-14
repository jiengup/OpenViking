// @vitest-environment jsdom
import { act, renderHook } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { useUserList } from './use-user-list'

const users = Array.from({ length: 2833 }, (_, index) => ({
  accountId: 'account',
  userId: `user-${index + 1}`,
  role: 'user',
}))

describe('user list', () => {
  it('can reach every user, including the partial last page', () => {
    const { result } = renderHook(() => useUserList(users, 'account'))
    expect(result.current.users).toHaveLength(20)
    act(() => result.current.setPage(result.current.pageCount))
    expect(result.current.page).toBe(142)
    expect(result.current.users).toHaveLength(13)
    expect(result.current.users.at(-1)?.userId).toBe('user-2833')
    act(() => result.current.setPageSize(100))
    expect(result.current.page).toBe(1)
    expect(result.current.pageCount).toBe(29)
    expect(result.current.users).toHaveLength(100)
  })

  it('searches beyond the current page and resets pagination', () => {
    const { result } = renderHook(() => useUserList(users, 'account'))
    act(() => result.current.setPage(10))
    act(() => result.current.setSearch(' USER-2833 '))
    expect(result.current.page).toBe(1)
    expect(result.current.total).toBe(1)
    expect(result.current.users[0].userId).toBe('user-2833')
    act(() => result.current.setSearch('missing'))
    expect(result.current.users).toEqual([])
    expect(result.current.pageCount).toBe(1)
    act(() => result.current.setSearch(''))
    expect(result.current.total).toBe(2833)
  })

  it('clamps the page after deletion and resets when switching accounts', () => {
    const { result, rerender } = renderHook(
      ({ data, scope }) => useUserList(data, scope),
      { initialProps: { data: users.slice(0, 21), scope: 'account' } },
    )
    act(() => result.current.setPage(2))
    rerender({ data: users.slice(0, 20), scope: 'account' })
    expect(result.current.page).toBe(1)
    expect(result.current.users).toHaveLength(20)
    act(() => result.current.setSearch('missing'))
    rerender({ data: users, scope: 'other-account' })
    expect(result.current.search).toBe('')
    expect(result.current.page).toBe(1)
    expect(result.current.users).toHaveLength(20)
  })
})
