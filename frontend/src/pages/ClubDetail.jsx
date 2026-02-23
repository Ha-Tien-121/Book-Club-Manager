import { useParams, Link } from "react-router-dom";
import {
  ArrowLeft,
  MapPin,
  Calendar,
  Users,
  BookOpen,
  MessageSquare,
  ExternalLink,
} from "lucide-react";
import { clubs } from "../data/mockData";

function formatDate(dateStr) {
  return new Date(dateStr).toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export default function ClubDetail() {
  const { id } = useParams();
  const club = clubs.find((c) => c.id === Number(id));

  if (!club) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-20 text-center">
        <p className="text-warm-400 text-lg">Club not found.</p>
        <Link to="/explore" className="text-sage-600 hover:text-sage-700 text-sm underline mt-3 inline-block">
          Back to Explore
        </Link>
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
      <Link
        to="/my-clubs"
        className="inline-flex items-center gap-1.5 text-sm text-warm-500 hover:text-warm-700 transition-colors mb-6"
      >
        <ArrowLeft className="w-4 h-4" />
        Back to My Clubs
      </Link>

      {/* Hero */}
      <div className="rounded-2xl overflow-hidden h-56 sm:h-72 relative mb-8">
        <img
          src={club.thumbnail}
          alt={club.name}
          className="w-full h-full object-cover"
        />
        <div className="absolute inset-0 bg-gradient-to-t from-warm-900/60 to-transparent" />
        <div className="absolute bottom-0 left-0 p-6 sm:p-8">
          <span className="px-3 py-1 text-xs font-medium rounded-full bg-warm-50/90 text-warm-700 mb-3 inline-block">
            {club.genre}
          </span>
          <h1 className="text-2xl sm:text-3xl font-serif font-bold text-white">
            {club.name}
          </h1>
        </div>
      </div>

      <div className="grid lg:grid-cols-3 gap-8">
        {/* Main content */}
        <div className="lg:col-span-2 space-y-8">
          <section>
            <h2 className="text-lg font-serif font-semibold text-warm-800 mb-3">
              About
            </h2>
            <p className="text-warm-600 leading-relaxed">
              {club.description}
            </p>
          </section>

          {/* Currently Reading */}
          {club.currentBook && (
            <section className="p-5 bg-warm-50 rounded-xl border border-warm-200">
              <h2 className="text-lg font-serif font-semibold text-warm-800 mb-4">
                Currently Reading
              </h2>
              <Link
                to={`/book/${club.currentBook.id}`}
                className="flex gap-4 group"
              >
                <img
                  src={club.currentBook.cover}
                  alt={club.currentBook.title}
                  className="w-20 rounded-lg shadow-sm"
                />
                <div>
                  <h3 className="font-serif font-semibold text-warm-900 group-hover:text-sage-700 transition-colors">
                    {club.currentBook.title}
                  </h3>
                  <p className="text-sm text-warm-500 mt-1">
                    {club.currentBook.author}
                  </p>
                  <div className="flex flex-wrap gap-1.5 mt-2">
                    {club.currentBook.genres.map((g) => (
                      <span
                        key={g}
                        className="px-2 py-0.5 text-xs rounded-full bg-sage-50 text-sage-600 border border-sage-200"
                      >
                        {g}
                      </span>
                    ))}
                  </div>
                </div>
              </Link>
            </section>
          )}

          {/* Discussion placeholder */}
          <section className="p-5 bg-warm-50 rounded-xl border border-warm-200">
            <div className="flex items-center gap-2 mb-4">
              <MessageSquare className="w-5 h-5 text-warm-400" />
              <h2 className="text-lg font-serif font-semibold text-warm-800">
                Club Discussion
              </h2>
            </div>
            <p className="text-warm-500 text-sm">
              Club forum coming soon. Members will be able to discuss chapters,
              coordinate schedules, and share thoughts here.
            </p>
          </section>
        </div>

        {/* Sidebar */}
        <aside className="space-y-6">
          <div className="p-5 bg-warm-50 rounded-xl border border-warm-200 space-y-4">
            <div className="flex items-center gap-3 text-sm text-warm-600">
              <MapPin className="w-4 h-4 text-warm-400 shrink-0" />
              {club.location}
            </div>
            <div className="flex items-center gap-3 text-sm text-warm-600">
              <Calendar className="w-4 h-4 text-warm-400 shrink-0" />
              {club.meetingDay} at {club.meetingTime}
            </div>
            <div className="flex items-center gap-3 text-sm text-warm-600">
              <Users className="w-4 h-4 text-warm-400 shrink-0" />
              {club.members} members
            </div>
          </div>

          {/* Next Meeting */}
          <div className="p-5 bg-sage-50 rounded-xl border border-sage-200">
            <p className="text-xs text-sage-500 uppercase tracking-wider mb-2">
              Next Meeting
            </p>
            <p className="text-sm font-medium text-sage-700">
              {formatDate(club.nextMeeting)}
            </p>
          </div>

          {club.isExternal && club.externalLink && (
            <a
              href={club.externalLink}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center justify-center gap-2 w-full px-4 py-2.5 bg-sage-600 text-white rounded-xl text-sm font-medium hover:bg-sage-700 transition-colors"
            >
              <ExternalLink className="w-4 h-4" />
              Visit Club Page
            </a>
          )}
        </aside>
      </div>
    </div>
  );
}
