import { useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';

import { resetPassword } from '../api/auth';

// ---------------------------------------------------------------------------
// Password strength rules
// ---------------------------------------------------------------------------

const PASSWORD_RULES = [
  { test: (v: string) => v.length >= 8, message: 'At least 8 characters' },
  { test: (v: string) => /[A-Z]/.test(v), message: 'At least one uppercase letter' },
  { test: (v: string) => /[0-9]/.test(v), message: 'At least one digit' },
  { test: (v: string) => /[^A-Za-z0-9]/.test(v), message: 'At least one special character' },
];

function validatePassword(value: string): string {
  for (const rule of PASSWORD_RULES) {
    if (!rule.test(value)) return rule.message;
  }
  return '';
}

// ---------------------------------------------------------------------------

export function ResetPasswordPage() {
  const [searchParams] = useSearchParams();
  const token = searchParams.get('token');

  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [passwordError, setPasswordError] = useState('');
  const [confirmError, setConfirmError] = useState('');
  const [loading, setLoading] = useState(false);
  const [succeeded, setSucceeded] = useState(false);
  const [submitError, setSubmitError] = useState('');
  const [tokenInvalid, setTokenInvalid] = useState(false);

  // No token in URL — show error immediately
  if (!token) {
    return (
      <div className="min-h-screen bg-gray-100 dark:bg-gray-900 flex items-center justify-center">
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md w-full max-w-sm p-8">
          <h1 className="text-2xl font-bold text-gray-800 dark:text-gray-100 mb-4">Invalid Link</h1>
          <p className="text-sm text-red-600 dark:text-red-400 mb-6">Invalid reset link.</p>
          <Link
            to="/forgot-password"
            className="text-xs text-blue-600 hover:text-blue-700 dark:text-blue-400 dark:hover:text-blue-300"
          >
            Request a new reset link
          </Link>
        </div>
      </div>
    );
  }

  const handleNewPasswordChange = (value: string) => {
    setNewPassword(value);
    if (passwordError) setPasswordError(validatePassword(value));
    if (confirmError && confirmPassword) {
      setConfirmError(value !== confirmPassword ? 'Passwords do not match' : '');
    }
  };

  const handleConfirmPasswordChange = (value: string) => {
    setConfirmPassword(value);
    if (confirmError) {
      setConfirmError(value !== newPassword ? 'Passwords do not match' : '');
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitError('');

    // Client-side validation
    const pwdErr = validatePassword(newPassword);
    if (pwdErr) {
      setPasswordError(pwdErr);
      return;
    }
    if (newPassword !== confirmPassword) {
      setConfirmError('Passwords do not match');
      return;
    }

    setLoading(true);
    try {
      await resetPassword(token, newPassword);
      setSucceeded(true);
    } catch (err: unknown) {
      const status = (err as { status?: number }).status;
      if (status === 400) {
        setTokenInvalid(true);
      } else {
        setSubmitError('Something went wrong. Please try again.');
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-100 dark:bg-gray-900 flex items-center justify-center">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md w-full max-w-sm p-8">
        <h1 className="text-2xl font-bold text-gray-800 dark:text-gray-100 mb-2">Set New Password</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-6">
          Choose a strong password for your account.
        </p>

        {succeeded && (
          <div className="space-y-4">
            <p className="text-sm text-green-600 dark:text-green-400">Password updated successfully.</p>
            <Link
              to="/login"
              className="text-xs text-blue-600 hover:text-blue-700 dark:text-blue-400 dark:hover:text-blue-300"
            >
              Go to Login
            </Link>
          </div>
        )}

        {tokenInvalid && (
          <div className="space-y-4">
            <p className="text-sm text-red-600 dark:text-red-400">
              This reset link is invalid or has expired.
            </p>
            <Link
              to="/forgot-password"
              className="text-xs text-blue-600 hover:text-blue-700 dark:text-blue-400 dark:hover:text-blue-300"
            >
              Request a new reset link
            </Link>
          </div>
        )}

        {!succeeded && !tokenInvalid && (
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                New Password
              </label>
              <input
                type="password"
                value={newPassword}
                onChange={(e) => handleNewPasswordChange(e.target.value)}
                required
                autoFocus
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              {passwordError && (
                <p className="mt-1 text-xs text-red-600 dark:text-red-400">{passwordError}</p>
              )}
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Confirm Password
              </label>
              <input
                type="password"
                value={confirmPassword}
                onChange={(e) => handleConfirmPasswordChange(e.target.value)}
                required
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              {confirmError && (
                <p className="mt-1 text-xs text-red-600 dark:text-red-400">{confirmError}</p>
              )}
            </div>

            {submitError && <p className="text-sm text-red-600">{submitError}</p>}

            <button
              type="submit"
              disabled={loading}
              className="w-full bg-blue-600 text-white py-2 rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
            >
              {loading ? 'Saving...' : 'Set New Password'}
            </button>
          </form>
        )}
      </div>
    </div>
  );
}
