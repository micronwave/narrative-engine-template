const mockPush = jest.fn();
const mockReplace = jest.fn();
const mockBack = jest.fn();

export const useRouter = () => ({
  push: mockPush,
  replace: mockReplace,
  back: mockBack,
  prefetch: jest.fn(),
});

export const useParams = () => ({ id: "test-narrative-id" });
export const usePathname = () => "/";
export const useSearchParams = () => new URLSearchParams();
export { mockPush, mockReplace };
