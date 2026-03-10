import { useParams, Link } from "react-router-dom";
import { useState } from "react";
import {
  ArrowLeft,
  Star,
  Users,
  MapPin,
  BookOpen,
  BookMarked,
  CheckCircle2,
  ChevronDown,
} from "lucide-react";
import { books, clubs } from "../data/mockData";

const statusOptions = [
  { key: "saved", label: "Save for Later", icon: BookMarked },
  { key: "inProgress", label: "Currently Reading", icon: BookOpen },
  { key: "finished", label: "Finished", icon: CheckCircle2 },
];

export default function BookDetail() {
  const { id } = useParams();
  const book = books.find((b) => b.id === Number(id));
  const [savedStatus, setSavedStatus] = useState(null);
  const [showStatusMenu, setShowStatusMenu] = useState(false);

  if (!book) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-20 text-center">
        <p className="text-warm-400 text-lg">Book not found.</p>
        <Link to="/" className="text-sage-600 hover:text-sage-700 text-sm underline mt-3 inline-block">
          Back to Feed
        </Link>
      </div>
    );
  }

  const clubsReadingThis = clubs.filter(
    (c) => c.currentBook?.id === book.id
  );

  const currentStatus = statusOptions.find((s) => s.key === savedStatus);

  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
      <Link
        to="/"
        className="inline-flex items-center gap-1.5 text-sm text-warm-500 hover:text-warm-700 transition-colors mb-8"
      >
        <ArrowLeft className="w-4 h-4" />
        Back to Feed
      </Link>

      <div className="flex flex-col sm:flex-row gap-8">
        {/* Cover */}
        <div className="sm:w-64 shrink-0">
          <div className="aspect-[2/3] rounded-2xl overflow-hidden shadow-lg bg-warm-100">
            <img
              src={book.cover}
              alt={book.title}
              className="w-full h-full object-cover"
            />
          </div>
        </div>

        {/* Info */}
        <div className="flex-1 min-w-0">
          <div className="flex flex-wrap gap-2 mb-3">
            {book.genres.map((g) => (
              <span
                key={g}
                className="px-2.5 py-1 text-xs rounded-full bg-sage-50 text-sage-600 border border-sage-200"
              >
                {g}
              </span>
            ))}
          </div>

          <h1 className="text-3xl font-serif font-bold text-warm-900">
            {book.title}
          </h1>
          <p className="text-lg text-warm-500 mt-1">{book.author}</p>

          {/* Stats */}
          <div className="flex items-center gap-5 mt-4 text-sm text-warm-600">
            <span className="flex items-center gap-1.5">
              <Star className="w-4 h-4 text-amber-500 fill-amber-500" />
              {book.rating}
              <span className="text-warm-400">
                ({book.ratingCount.toLocaleString()} ratings)
              </span>
            </span>
            <span className="flex items-center gap-1.5">
              <Users className="w-4 h-4 text-warm-400" />
              {book.clubsReading} clubs reading
            </span>
          </div>

          <p className="mt-6 text-warm-600 leading-relaxed">
            {book.description}
          </p>

          {/* Save Button */}
          <div className="mt-6 relative">
            <button
              onClick={() => setShowStatusMenu(!showStatusMenu)}
              className={`flex items-center gap-2 px-5 py-2.5 rounded-xl text-sm font-medium transition-all ${
                savedStatus
                  ? "bg-sage-100 text-sage-700 border border-sage-200"
                  : "bg-sage-600 text-white hover:bg-sage-700"
              }`}
            >
              {currentStatus ? (
                <>
                  <currentStatus.icon className="w-4 h-4" />
                  {currentStatus.label}
                </>
              ) : (
                <>
                  <BookMarked className="w-4 h-4" />
                  Save to Library
                </>
              )}
              <ChevronDown className="w-4 h-4 ml-1" />
            </button>

            {showStatusMenu && (
              <div className="absolute top-full left-0 mt-2 bg-warm-50 rounded-xl border border-warm-200 shadow-lg overflow-hidden z-10 w-56">
                {statusOptions.map(({ key, label, icon: Icon }) => (
                  <button
                    key={key}
                    onClick={() => {
                      setSavedStatus(key === savedStatus ? null : key);
                      setShowStatusMenu(false);
                    }}
                    className={`flex items-center gap-3 w-full px-4 py-3 text-sm text-left transition-colors ${
                      savedStatus === key
                        ? "bg-sage-50 text-sage-700"
                        : "text-warm-600 hover:bg-warm-100"
                    }`}
                  >
                    <Icon className="w-4 h-4" />
                    {label}
                    {savedStatus === key && (
                      <CheckCircle2 className="w-4 h-4 ml-auto text-sage-500" />
                    )}
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* SPL Availability */}
      {book.splAvailable && (
        <section className="mt-10 p-5 bg-warm-50 rounded-xl border border-warm-200">
          <div className="flex items-center gap-2 mb-4">
            <MapPin className="w-5 h-5 text-sage-500" />
            <h2 className="text-lg font-serif font-semibold text-warm-800">
              Available at Seattle Public Library
            </h2>
          </div>
          <div className="flex flex-wrap gap-2">
            {book.splBranches.map((branch) => (
              <span
                key={branch}
                className="px-3 py-1.5 text-sm rounded-lg bg-sage-50 text-sage-600 border border-sage-200"
              >
                {branch}
              </span>
            ))}
          </div>
          <p className="text-sm text-warm-500 mt-3">
            Checked out {book.checkouts.toLocaleString()} times in the past year
          </p>
        </section>
      )}

      {/* Clubs Reading This */}
      {clubsReadingThis.length > 0 && (
        <section className="mt-8">
          <h2 className="text-lg font-serif font-semibold text-warm-800 mb-4">
            Clubs Reading This Book
          </h2>
          <div className="space-y-3">
            {clubsReadingThis.map((club) => (
              <Link
                key={club.id}
                to={`/club/${club.id}`}
                className="flex items-center gap-4 p-4 bg-warm-50 rounded-xl border border-warm-200 hover:border-warm-300 hover:shadow-sm transition-all"
              >
                <img
                  src={club.thumbnail}
                  alt={club.name}
                  className="w-12 h-12 rounded-lg object-cover"
                />
                <div>
                  <p className="font-medium text-warm-800">{club.name}</p>
                  <p className="text-sm text-warm-500">
                    {club.members} members &middot; {club.location}
                  </p>
                </div>
              </Link>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
