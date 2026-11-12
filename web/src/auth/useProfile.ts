import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import { useToken } from './session';

export type Profile = { id: string; email: string; full_name: string; bio: string; role: 'user' | 'ops' };

// useProfile is the signed-in user's profile (auth-api, through the
// console), shared by the Profile page, the nav and the ops guard. Nothing is
// fetched while signed out.
export function useProfile() {
  const token = useToken();
  return useQuery({ queryKey: ['profile'], queryFn: () => api<Profile>('/profile', { auth: true }), enabled: !!token });
}
