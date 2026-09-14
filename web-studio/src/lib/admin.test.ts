import { describe, expect, it, vi } from 'vitest'
import type * as OvClient from '#/lib/ov-client'

import { fetchAdminUsers } from './admin'

vi.mock('#/lib/ov-client', async (importOriginal) => {
  const original = await importOriginal<typeof OvClient>()
  return {
    ...original,
    getAdminAccountIdUsers: vi.fn(async (options) => {
      const users = Array.from({ length: 2833 }, (_, index) => ({
        user_id: `user-${index + 1}`,
        role: index === 2832 ? 'admin' : 'user',
      }))
      return {
        headers: {},
        status: 200,
        data: {
          status: 'ok',
          result: users.slice(0, options.query?.limit),
        },
      }
    }),
  }
})

describe('fetchAdminUsers', () => {
  it('includes users and administrators beyond the first 500 members', async () => {
    const users = await fetchAdminUsers(
      {
        accountId: 'default',
        apiKey: 'test-key',
        baseUrl: 'http://localhost:1933',
        userId: 'root',
      },
      'customer_agent_as',
    )

    expect(users).toHaveLength(2833)
    expect(users.at(-1)).toMatchObject({
      accountId: 'customer_agent_as',
      userId: 'user-2833',
      role: 'admin',
    })
  })
})
