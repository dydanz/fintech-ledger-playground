import { useEffect, useState, type FormEvent } from 'react';
import { useNavigate } from 'react-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api } from '../api/client';
import { session } from '../auth/session';
import { ErrorLine } from '../auth/Notice';

type Profile = { id: string; email: string; full_name: string; bio: string };

export function Profile() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const profile = useQuery({ queryKey: ['profile'], queryFn: () => api<Profile>('/profile', { auth: true }) });

  const [fullName, setFullName] = useState('');
  const [bio, setBio] = useState('');
  useEffect(() => {
    if (profile.data) {
      setFullName(profile.data.full_name);
      setBio(profile.data.bio);
    }
  }, [profile.data]);

  const save = useMutation({
    mutationFn: () => api<Profile>('/profile', { method: 'PUT', auth: true, body: { full_name: fullName, bio } }),
    onSuccess: (p) => queryClient.setQueryData(['profile'], p),
  });

  const [current, setCurrent] = useState('');
  const [next, setNext] = useState('');
  // A password change ends every session, this one included (auth-api
  // C1.1), so the user logs in again with the new password.
  const change = useMutation({
    mutationFn: () => api('/password', { method: 'PUT', auth: true, body: { current_password: current, new_password: next } }),
    onSuccess: () => {
      session.end('Password changed. Every session was logged out; log in with the new password.');
      queryClient.clear();
      navigate('/login', { replace: true });
    },
  });

  if (profile.isPending) return <p className="sub">Loading your profile…</p>;
  if (profile.isError) return <ErrorLine error={profile.error} />;

  return (
    <section className="narrow">
      <h1>Profile</h1>
      <p className="sub">{profile.data.email}</p>
      <form
        className="form"
        onSubmit={(e: FormEvent) => {
          e.preventDefault();
          save.mutate();
        }}
      >
        <h2>Details</h2>
        <label>
          Full name
          <input value={fullName} onChange={(e) => setFullName(e.target.value)} />
        </label>
        <label>
          Bio
          <textarea value={bio} onChange={(e) => setBio(e.target.value)} />
        </label>
        <ErrorLine error={save.error} />
        {save.isSuccess && <p className="notice">Saved.</p>}
        <button type="submit" disabled={save.isPending}>
          Save
        </button>
      </form>
      <form
        className="form"
        onSubmit={(e: FormEvent) => {
          e.preventDefault();
          change.mutate();
        }}
      >
        <h2>Change password</h2>
        <p className="sub">This logs you out everywhere, including here.</p>
        <label>
          Current password
          <input type="password" autoComplete="current-password" required value={current} onChange={(e) => setCurrent(e.target.value)} />
        </label>
        <label>
          New password <span className="hint">8 to 72 characters</span>
          <input type="password" autoComplete="new-password" required minLength={8} value={next} onChange={(e) => setNext(e.target.value)} />
        </label>
        <ErrorLine error={change.error} />
        <button type="submit" disabled={change.isPending}>
          Change password
        </button>
      </form>
    </section>
  );
}
